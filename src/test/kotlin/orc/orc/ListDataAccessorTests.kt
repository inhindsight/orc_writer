package orc.orc

import orc.ListDataAccessor
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector
import org.apache.orc.TypeDescription
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertIterableEquals
import org.junit.jupiter.api.Test

@Suppress("UNCHECKED_CAST")
class ListDataAccessorTests {

    @Test
    fun `can write lists`() {
        val schema = TypeDescription.createList(TypeDescription.createString())
        val bytesColumnVector = BytesColumnVector()
        bytesColumnVector.init()
        val columnVector = ListColumnVector(100, bytesColumnVector)
        columnVector.init()

        val accessor = ListDataAccessor(schema, columnVector)

        val data = otp.list(
            otp.string("one"),
            otp.string("two")
        )

        accessor.set(0, data)

        assertEquals("one", bytesColumnVector.toString(0))
        assertEquals("two", bytesColumnVector.toString(1))
    }

    @Test
    fun `can read lists`() {
        val schema = TypeDescription.createList(TypeDescription.createString())
        val bytesColumnVector = BytesColumnVector()
        bytesColumnVector.init()
        val columnVector = ListColumnVector(100, bytesColumnVector)
        columnVector.init()

        columnVector.offsets[0] = 0
        columnVector.lengths[0] = 3
        columnVector.childCount = 3

        bytesColumnVector.setVal(0, "one".toByteArray())
        bytesColumnVector.setVal(1, "two".toByteArray())
        bytesColumnVector.setVal(2, "three".toByteArray())

        val accessor = ListDataAccessor(schema, columnVector)

        val expected = listOf("one", "two", "three")

        assertIterableEquals(expected, accessor.get(0) as List<Any>)
    }
}