package orc

import com.ericsson.otp.erlang.OtpErlangList
import com.ericsson.otp.erlang.OtpErlangObject
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector
import org.apache.orc.TypeDescription

class ListDataAccessor(private val schema: TypeDescription, private val columnVector: ListColumnVector): DataAccessor {

    val accessor = DataAccessor.create(schema, arrayOf(columnVector.child)).first()

    override fun get(row: Int): Any {
        val start = columnVector.offsets[row]
        val end = start + columnVector.lengths[row]

        return (start until end).fold(listOf<Any>(), {acc, subRow ->
            acc + accessor.get(subRow.toInt())
        })
    }

    override fun set(row: Int, value: OtpErlangObject) {
        val array = (value as OtpErlangList).elements()
        val start = columnVector.childCount
        columnVector.offsets[row] = start.toLong()
        columnVector.lengths[row] = array.size.toLong()

        array.forEach {
            val subRow = columnVector.childCount++
            accessor.set(subRow, it)
        }
    }
}