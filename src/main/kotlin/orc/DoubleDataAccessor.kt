package orc

import com.ericsson.otp.erlang.OtpErlangDouble
import com.ericsson.otp.erlang.OtpErlangObject
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector

class DoubleDataAccessor(private val columnVector: DoubleColumnVector) : DataAccessor {
    override fun get(row: Int): Any = columnVector.vector[row]

    override fun set(row: Int, value: OtpErlangObject) {
        columnVector.vector[row] = (value as OtpErlangDouble).doubleValue()
    }
}