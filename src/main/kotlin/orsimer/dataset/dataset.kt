package orsimer.dataset

import com.ericsson.otp.erlang.OtpErlangList
import com.ericsson.otp.erlang.OtpErlangTuple
import com.ericsson.otp.erlang.OtpMbox
import com.ericsson.otp.erlang.OtpNode
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.orc.TypeDescription
import otp.asAtom

fun start(node: OtpNode, dataset_id: String, subset_id: String, schemaString: String) = GlobalScope.launch {
    val id = "${dataset_id}__${subset_id}"
    val mailbox = node.createMbox(id)

    val schema = TypeDescription.fromString(schemaString)

    loop(mailbox, schema)
}

private tailrec suspend fun loop(mailbox: OtpMbox, schema: TypeDescription) {
    val otpMsg = withContext(IO) {
        mailbox.receiveMsg()
    }

    try {
        val tuple = otpMsg.msg as OtpErlangTuple
        val command = tuple.elementAt(0).asAtom()

        when (command) {
            "ping" -> {
                mailbox.send(otpMsg.senderPid, otp.atom("pong"))
            }
            "write" -> {
                val data = tuple.elementAt(1) as OtpErlangList
                val path = orc.write(schema, data)

                val response = otp.tuple(
                    otp.atom("ok"),
                    otp.string(path.toString())
                )

                mailbox.send(otpMsg.senderPid, response)
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

    loop(mailbox, schema)
}
