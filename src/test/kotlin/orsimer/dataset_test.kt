package orsimer

import com.ericsson.otp.erlang.OtpErlangAtom
import com.ericsson.otp.erlang.OtpErlangList
import com.ericsson.otp.erlang.OtpErlangTuple
import com.ericsson.otp.erlang.OtpNode
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.awaitility.Awaitility
import org.awaitility.kotlin.untilNotNull
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DatasetTest {

    private val node = OtpNode("orsimer@127.0.0.1")
    private val mailbox = node.createMbox()

    @BeforeEach
    fun beforeEach() {
        GlobalScope.launch {
            orsimer.dataset.start(node, "ds1", "sb1")
        }

        Awaitility.await().untilNotNull { node.whereis("ds1__sb1") }
    }

    @Test
    fun `is pingable`() {
        val msg = otp.tuple(
            otp.atom("ping"),
            mailbox.self()
        )

        mailbox.send("ds1__sb1", "orsimer@127.0.0.1", msg)

        val response = mailbox.receive(10_000) as OtpErlangAtom
        assertEquals("pong", response.atomValue())
    }

    @Test
    fun `can receive a write call`() {
        val msg = otp.tuple(
            otp.atom("write"),
            mailbox.self(),
            otp.list(
                otp.map(
                    "name" to otp.string("george"),
                    "age" to otp.long(10)
                ),
                otp.map(
                    "name" to otp.string("bob"),
                    "age" to otp.long(11)
                )
            )
        )
    }
}
