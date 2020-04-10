@file:Suppress("UNCHECKED_CAST")

package orc

import org.apache.hadoop.fs.Path
import org.apache.orc.TypeDescription
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File

class OrcTest {

    var filename: Path? = null

    @AfterEach
    fun cleanup() {
        if (filename != null) {
            File(filename.toString()).delete()
        }
    }

    @Test
    fun `orc can write simple values`() {
        val spouseSchema = TypeDescription.createStruct()
            .addField("name", TypeDescription.createString())
            .addField("age", TypeDescription.createLong())

        val schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField("name", TypeDescription.createString())
            .addField("double", TypeDescription.createDouble())
            .addField("created", TypeDescription.createBoolean())
            .addField("birthdate", TypeDescription.createDate())
            .addField("ts", TypeDescription.createTimestamp())
            .addField("spouse", spouseSchema)

        val data = otp.list(
            otp.map(
                "id" to otp.long(1L),
                "name" to otp.string("george"),
                "double" to otp.double(7.4),
                "created" to otp.boolean(true),
                "birthdate" to otp.string("2020-04-10"),
                "ts" to otp.string("2020-04-10T11:51:57.217078"),
                "spouse" to otp.map("name" to otp.string("jenny"), "age" to otp.long(56))
            )
        )

        filename = write(schema, data)
        val contents = read(filename!!)[0]

        assertEquals(1L, contents["id"])
        assertEquals("george", contents["name"])
        assertEquals(7.4, contents["double"] as Double, 0.00001)
        assertEquals(true, contents["created"])
        assertEquals("2020-04-10", contents["birthdate"])
        assertEquals("2020-04-10T11:51:57.217078", contents["ts"])

        val spouse = contents["spouse"] as Map<String, Any>
        assertEquals("jenny", spouse["name"])
        assertEquals(56L, spouse["age"])
    }
}
