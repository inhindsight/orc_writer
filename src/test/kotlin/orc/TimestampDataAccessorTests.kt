package orc

import com.ericsson.otp.erlang.OtpErlangBinary
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.Month
import java.time.format.DateTimeFormatter

class TimestampDataAccessorTests {

    val columnVector = TimestampColumnVector()
    val accessor = TimestampDataAccessor(columnVector)

    @Test
    fun `will set timestamp properly`() {
        val timestamp = OtpErlangBinary("2020-04-10T11:51:57.217078".toByteArray())
        accessor.set(0, timestamp)

        val dt = LocalDateTime.of(2020, Month.APRIL,10, 11, 51, 57, 217078000)
        val expected = Timestamp.valueOf(dt)
        assertEquals(expected.time, columnVector.getTime(0))
    }

    @Test
    fun `will get timestamp from vector`() {
        val dateTime = LocalDateTime.now()
        columnVector.set(0, Timestamp.valueOf(dateTime))

        val expected = dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

        assertEquals(expected, accessor.get(0))
    }
}