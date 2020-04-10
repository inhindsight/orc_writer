package orc

import com.ericsson.otp.erlang.OtpErlangObject
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector
import otp.asString
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class TimestampDataAccessor(private val columnVector: TimestampColumnVector) : DataAccessor {
    override fun get(row: Int): Any {
        val ts = Timestamp(columnVector.getTime(row))
        ts.nanos = columnVector.getNanos(row)
        return ts.toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    }

    override fun set(row: Int, value: OtpErlangObject) {
        val string = value.asString()
        val dateTime = LocalDateTime.parse(string, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        columnVector.set(row, Timestamp.valueOf(dateTime))
    }
}