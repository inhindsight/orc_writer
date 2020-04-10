package orc

import com.ericsson.otp.erlang.OtpErlangBinary
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class DateDataAccessorTests {

    private val columnVector = LongColumnVector()
    private val accessor = DateDataAccessor(columnVector)

    @Test
    fun `will set date as days since epoch`() {
        val date = OtpErlangBinary("1977-05-27".toByteArray())
        accessor.set(0, date)

        assertEquals(2703L, columnVector.vector[0])
    }

    @Test
    fun `will get date back as date string`() {
        columnVector.vector[0] = 2703L
        assertEquals("1977-05-27", accessor.get(0))
    }
}