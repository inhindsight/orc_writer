package orsimer.dataset

import com.ericsson.otp.erlang.*
import kotlinx.coroutines.withContext
import kotlinx.coroutines.Dispatchers.IO
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import orsimer.Command

fun start(node: OtpNode, dataset_id: String, subset_id: String) = GlobalScope.launch {
    val id = "${dataset_id}__${subset_id}"
    val mailbox = node.createMbox(id)

    val initialState = mapOf<String, Int>()
    loop(mailbox, initialState)
}

private tailrec suspend fun loop(mailbox: OtpMbox, state: Map<String, Int>) {
    val otpMsg = withContext(IO) {
        mailbox.receiveMsg()
    }

    val command = Command.fromMessage(otpMsg.msg)

    when (command.action) {
        "ping" -> {
            mailbox.send(otpMsg.senderPid, otp.atom("pong"))
            loop(mailbox, state)
        }
    }
}
