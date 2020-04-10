package orsimer

import com.ericsson.otp.erlang.*
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.hadoop.fs.Path
import org.apache.orc.TypeDescription
import org.awaitility.Awaitility
import org.awaitility.kotlin.untilNotNull
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertIterableEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import otp.asString
import java.io.File
import kotlin.system.measureTimeMillis

class DatasetTest {

    private val node = OtpNode("orsimer@127.0.0.1")
    private val mailbox = node.createMbox()

    @BeforeEach
    fun beforeEach() {
        val schema = TypeDescription.createStruct()
            .addField("name", TypeDescription.createString())
            .addField("age", TypeDescription.createLong())
        GlobalScope.launch {
            orsimer.dataset.start(node, "ds1", "sb1", schema.toString())
        }

        Awaitility.await().untilNotNull { node.whereis("ds1__sb1") }
    }

    @AfterEach
    fun afterEach() {
        node.close()
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
            otp.list(
                otp.map("name" to otp.string("joe"), "age" to otp.long(10)),
                otp.map("name" to otp.string("jerry"), "age" to otp.long(12))
            )
        )

        mailbox.send("ds1__sb1", "orsimer@127.0.0.1", msg)

        val response = mailbox.receive(10_000) as OtpErlangTuple
        assertEquals(otp.atom("ok"), response.elementAt(0))
        val filePath = response.elementAt(1).asString()

        val output = orc.read(Path(filePath))

        assertEquals("joe", output[0]["name"])
        assertEquals(10L, output[0]["age"])
        assertEquals("jerry", output[1]["name"])
        assertEquals(12L, output[1]["age"])

        File(filePath).delete()
    }
}
