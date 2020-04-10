package orc

import com.ericsson.otp.erlang.OtpErlangObject
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import otp.asString
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS

class DateDataAccessor(private val columnVector: LongColumnVector) : DataAccessor {
    private val epoch = LocalDate.EPOCH

    override fun get(row: Int): Any {
        val days = columnVector.vector[row]
        val date = DAYS.addTo(epoch, days)
        return date.format(DateTimeFormatter.ISO_DATE)
    }

    override fun set(row: Int, value: OtpErlangObject) {
        val string = value.asString()
        val date = LocalDate.parse(string)
        columnVector.vector[row] = DAYS.between(epoch, date)
    }
}