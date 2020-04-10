package orc

import com.ericsson.otp.erlang.OtpErlangMap
import com.ericsson.otp.erlang.OtpErlangObject
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.orc.TypeDescription
import otp.asString

class StructDataAccessor(schema: TypeDescription, cols: Array<ColumnVector>) : DataAccessor {

    private val fields = schema.fieldNames.withIndex()
    private val accessors = DataAccessor.create(schema, cols)

    override fun get(row: Int): Any {
        return fields.fold(mapOf<String, Any>()) { map, field ->
            map + (field.value to accessors[field.index].get(row))
        }
    }

    override fun set(row: Int, value: OtpErlangObject) {
        val otpMap = value as OtpErlangMap
        val dataMap = otpMap.keys().associate { it.asString() to otpMap.get(it) }

        fields.forEach { field ->
            val fieldValue = dataMap.getOrElse(field.value, { throw IllegalArgumentException() })
            accessors[field.index].set(row, fieldValue)
        }
    }
}