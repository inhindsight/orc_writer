package orc

import com.ericsson.otp.erlang.OtpErlangList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcFile
import org.apache.orc.TypeDescription

private val conf = Configuration()

fun write(schema: TypeDescription, data: OtpErlangList): Path {
    val path = Path(System.nanoTime().toString())
    path.getFileSystem(conf).setWriteChecksum(false)
    val writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(schema))

    val batch = schema.createRowBatch()
    val accessor = StructDataAccessor(schema, batch.cols)

    data.elements().forEach {
        val row = batch.size++
        accessor.set(row, it)

        if (batch.size == batch.maxSize) {
            writer.addRowBatch(batch)
            batch.reset()
        }
    }

    if (batch.size != 0) {
        writer.addRowBatch(batch)
        batch.reset()
    }

    writer.close()

    return path
}

@Suppress("UNCHECKED_CAST")
fun read(path: Path): List<Map<String, Any>> {
    val reader = OrcFile.createReader(path, OrcFile.readerOptions(conf))
    val rows = reader.rows()
    val schema = reader.schema
    val batch = reader.schema.createRowBatch()

    val accessor = StructDataAccessor(schema, batch.cols)

    while (rows.nextBatch(batch)) {
        return (0 until batch.size).fold(listOf()) { acc, row ->
            acc + (accessor.get(row) as Map<String, Any>)
        }
    }

    return emptyList()
}
