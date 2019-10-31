package orsimer

import com.ericsson.otp.erlang.*

fun main(args: Array<String>) {
    server()
}

private fun server() {
    val node = OtpNode("orsimer@127.0.0.1")
    val mailbox = node.createMbox("main")

    val initialState = mapOf<String, Int>()
    loop(mailbox, initialState)
}

private fun loop(mailbox: OtpMbox, state: Map<String, Int>) {
    println(state)
    when (val msg = mailbox.receive()) {
        is OtpErlangString -> {
            val count = state.getOrDefault(msg.stringValue(), 0)
            loop(mailbox, state + listOf(msg.stringValue() to count + 1))
        }
        else -> loop(mailbox, state)
    }
}