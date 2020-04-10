package orc

import com.ericsson.otp.erlang.OtpErlangBoolean
import com.ericsson.otp.erlang.OtpErlangObject
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector

class BooleanDataAccessor(private val columnVector: LongColumnVector) : DataAccessor {
    override fun get(row: Int): Any = columnVector.vector[row] == 1L

    override fun set(row: Int, value: OtpErlangObject) {
        columnVector.vector[row] = if ((value as OtpErlangBoolean).booleanValue()) 1L else 0L;
    }
}