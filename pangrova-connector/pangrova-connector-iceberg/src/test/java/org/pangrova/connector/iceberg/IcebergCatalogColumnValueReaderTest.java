package org.pangrova.connector.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.fs.IcebergCatalogColumnValueReader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergCatalogColumnValueReaderTest {
    @TempDir Path warehouse;

    @Test
    void readsTargetFileWhenListingContainsDroppedAndSimilarlyNamedTables() throws IOException {
        HadoopTestGuard.assumeHadoopCompatible();
        Namespace namespace = Namespace.of("db");
        TableIdentifier vanished = TableIdentifier.of(namespace, "retired");
        TableIdentifier shorter = TableIdentifier.of(namespace, "orders");
        TableIdentifier target = TableIdentifier.of(namespace, "orders_archive");
        Schema unrelated = new Schema(Types.NestedField.required(1, "different_column", Types.LongType.get()));
        Schema targetSchema = new Schema(Types.NestedField.required(3, "id", Types.LongType.get()));

        try (var catalog = new HadoopCatalog(new Configuration(), warehouse.toString()) {
            @Override
            public List<TableIdentifier> listTables(Namespace listedNamespace) {
                assertThat(listedNamespace).isEqualTo(namespace);
                return List.of(vanished, shorter, target);
            }
        }) {
            catalog.createNamespace(namespace);
            catalog.createTable(vanished, unrelated);
            catalog.createTable(shorter, unrelated);
            var table = catalog.createTable(target, targetSchema);
            Schema schema = table.schema();
            assertThat(catalog.dropTable(vanished, false)).isTrue();
            String path = table.location() + "/data/orders.parquet";
            try (var appender = Parquet.write(table.io().newOutputFile(path))
                    .schema(schema).createWriterFunc(GenericParquetWriter::create).build()) {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", 17L);
                appender.add(record);
            }

            try (var reader = new IcebergCatalogColumnValueReader(catalog, table.io())) {
                var rows = reader.readColumns(path, List.of("id"));
                assertThat(rows.next()).containsExactly(17L);
                assertThat(rows.hasNext()).isFalse();
            }
        }
    }
}
