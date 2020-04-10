package orc

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.apache.orc.TypeDescription
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@Suppress("UNCHECKED_CAST")
class StructDataAccessorTest {

    @Test
    fun `can write map into struct`() {
        val schema = TypeDescription.createStruct()
            .addField("name", TypeDescription.createString())
            .addField("age", TypeDescription.createLong())

        val bytesColumnVector = BytesColumnVector()
        bytesColumnVector.init()
        val longColumnVector = LongColumnVector()

        val accessor = StructDataAccessor(schema, arrayOf(bytesColumnVector, longColumnVector))

        val data = otp.map(
            "name" to otp.string("mary"),
            "age" to otp.long(21)
        )

        accessor.set(0, data)

        assertEquals("mary", bytesColumnVector.toString(0))
        assertEquals(21L, longColumnVector.vector[0])
    }

    @Test
    fun `can read map from struct`() {
        val schema = TypeDescription.createStruct()
            .addField("name", TypeDescription.createString())
            .addField("age", TypeDescription.createLong())

        val bytesColumnVector = BytesColumnVector()
        bytesColumnVector.init()
        bytesColumnVector.setVal(0, "shannon".toByteArray())
        val longColumnVector = LongColumnVector()
        longColumnVector.vector[0] = 64L

        val accessor = StructDataAccessor(schema, arrayOf(bytesColumnVector, longColumnVector))
        val map = accessor.get(0) as Map<String, Any>

        assertEquals(map["name"],"shannon")
        assertEquals(map["age"], 64L)
    }
}