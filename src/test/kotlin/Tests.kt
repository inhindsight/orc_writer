import com.ericsson.otp.erlang.OtpNode
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import otp.asAtom


class Tests {

    @BeforeEach
    fun beforeEach() {
        GlobalScope.launch {
            orsimer.main(arrayOf())
        }
        Thread.sleep(1_000)
    }

    @Test
    fun `main server works`() {
        val testNode = OtpNode("test@127.0.0.1")
        val mailbox = testNode.createMbox("test")

        val msg = otp.tuple(
            otp.atom("start_dataset"),
            otp.string("ds1"),
            otp.string("sb1"),
            otp.string("struct<name:string>")
        )

        mailbox.send("main", "orsimer@127.0.0.1", msg)

        val response = mailbox.receive(10_000).asAtom()
        assertEquals("ok", response)

        Thread.sleep(2_000)

        mailbox.send(
            "ds1__sb1", "orsimer@127.0.0.1", otp.tuple(
                otp.atom("ping")
            )
        )

        val response2 = mailbox.receive(10_000).asAtom()
        assertEquals("pong", response2)

        testNode.close()
    }

}