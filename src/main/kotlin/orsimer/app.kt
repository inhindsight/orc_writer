package orsimer

import com.ericsson.otp.erlang.OtpMbox
import com.ericsson.otp.erlang.OtpNode
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import otp.asString

fun main(args: Array<String>) {
    server()
}

private fun server() = runBlocking<Unit> {
    val node = OtpNode("orsimer@127.0.0.1")
    val mailbox = node.createMbox("main")

    val initialState = mapOf<String, Int>()

    loop(node, mailbox, initialState)
}

private tailrec suspend fun loop(node: OtpNode, mailbox: OtpMbox, state: Map<String, Int>) {
    val otpMsg = withContext(IO) {
        mailbox.receiveMsg()
    }

    val command = Command.fromMessage(otpMsg.msg)

    when (command.action) {
        "start_dataset" -> {
            val (dataset, subset) = command.data
            orsimer.dataset.start(node, dataset.asString(), subset.asString())
            mailbox.send(otpMsg.senderPid, otp.ok())
            loop(node, mailbox, state)
        }
    }
}