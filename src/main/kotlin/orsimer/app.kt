package orsimer

import com.ericsson.otp.erlang.OtpErlangTuple
import com.ericsson.otp.erlang.OtpMbox
import com.ericsson.otp.erlang.OtpNode
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import otp.asAtom
import otp.asString

fun main(args: Array<String>) {
    server()
}

private fun server() = runBlocking<Unit> {
    val node = OtpNode("orsimer@127.0.0.1")
    val mailbox = node.createMbox("main")

    loop(node, mailbox)
}

private tailrec suspend fun loop(node: OtpNode, mailbox: OtpMbox) {
    val otpMsg = withContext(IO) {
        mailbox.receiveMsg()
    }

    try {
        val tuple = otpMsg.msg as OtpErlangTuple
        val command = tuple.elementAt(0).asAtom()

        when (command) {
            "start_dataset" -> {
                val datasetId = tuple.elementAt(1).asString()
                val subsetId = tuple.elementAt(2).asString()
                val schema = tuple.elementAt(3).asString()

                orsimer.dataset.start(node, datasetId, subsetId, schema)

                mailbox.send(otpMsg.senderPid, otp.ok())
            }
        }
    } catch (e: Exception) {
        mailbox.send(
            otpMsg.senderPid, otp.tuple(
                otp.atom("error"),
                otp.string(e.message!!)
            )
        )
    }

    loop(node, mailbox)
}