package orc

import com.ericsson.otp.erlang.OtpErlangLong
import com.ericsson.otp.erlang.OtpErlangObject
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector

class LongDataAccessor(private val columnVector: LongColumnVector) : DataAccessor {
    override fun get(row: Int): Any = columnVector.vector[row]

    override fun set(row: Int, value: OtpErlangObject) {
        columnVector.vector[row] = (value as OtpErlangLong).longValue()
    }
}