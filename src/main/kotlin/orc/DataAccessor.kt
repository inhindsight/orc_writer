package orc

import com.ericsson.otp.erlang.OtpErlangObject
import org.apache.hadoop.hive.ql.exec.vector.*
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category.*

interface DataAccessor {
    fun get(row: Int): Any
    fun set(row: Int, value: OtpErlangObject)

    companion object {
        fun create(schema: TypeDescription, cols: Array<ColumnVector>): List<DataAccessor> {
            val fields = schema.children.withIndex()
            return fields.fold(listOf()) { acc, field ->
                val columnVector = cols[field.index]
                val result = when (field.value.category) {
                    LONG -> LongDataAccessor(columnVector as LongColumnVector)
                    STRING -> StringDataAccessor(columnVector as BytesColumnVector)
                    DOUBLE -> DoubleDataAccessor(columnVector as DoubleColumnVector)
                    BOOLEAN -> BooleanDataAccessor(columnVector as LongColumnVector)
                    DATE -> DateDataAccessor(columnVector as LongColumnVector)
                    TIMESTAMP -> TimestampDataAccessor(columnVector as TimestampColumnVector)
                    STRUCT -> StructDataAccessor(field.value, (columnVector as StructColumnVector).fields)
                    LIST -> ListDataAccessor(field.value, columnVector as ListColumnVector)
                    else -> throw IllegalArgumentException(field.value.category.toString())
                }

                acc + result
            }
        }
    }
}
