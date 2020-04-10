package orc

import com.ericsson.otp.erlang.OtpErlangBinary
import com.ericsson.otp.erlang.OtpErlangObject
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector

class StringDataAccessor(private val columnVector: BytesColumnVector): DataAccessor {
    override fun get(row: Int): Any = columnVector.toString(row)

    override fun set(row: Int, value: OtpErlangObject) {
        val bytes = (value as OtpErlangBinary).binaryValue()
        columnVector.setVal(row, bytes)
    }
}