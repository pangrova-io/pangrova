package org.pangrova.connector.iceberg.read;

import org.pangrova.connector.iceberg.handle.IcebergColumnHandle;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import java.util.List;

/**
 * Opens Iceberg Parquet files with column projection via the Iceberg SDK reader.
 * Acts as anti-corruption layer: Parquet reader internals stay within this class.
 */
public final class IcebergParquetReader {

    private IcebergParquetReader() {}

    /**
     * Opens a Parquet file and returns a closeable iterable of records
     * with only the projected columns.
     */
    public static CloseableIterable<Record> open(
            Table table,
            String filePath,
            List<IcebergColumnHandle> projectedColumns) {
        Schema projectedSchema = buildProjectedSchema(table.schema(), projectedColumns);
        InputFile inputFile = table.io().newInputFile(filePath);
        return openParquetFile(inputFile, projectedSchema);
    }

    static Schema buildProjectedSchema(
            Schema fullSchema,
            List<IcebergColumnHandle> projectedColumns) {
        List<Types.NestedField> fields = projectedColumns.stream()
                .map(col -> fullSchema.findField(col.fieldId()))
                .toList();
        return new Schema(fields);
    }

    private static CloseableIterable<Record> openParquetFile(
            InputFile inputFile,
            Schema projectedSchema) {
        return Parquet.read(inputFile)
                .project(projectedSchema)
                .createReaderFunc(
                        fileSchema -> GenericParquetReaders.buildReader(
                                projectedSchema, fileSchema))
                .build();
    }
}
