package org.pangrova.testing.e2e.speculation;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Real bounded SDK writes establish the immutable source snapshot; no synthetic file statistics. */
final class ProcessInputSnapshot implements AutoCloseable {
    static final long ROW_COUNT = 8_053_064;
    static final String PAYLOAD = "actual-process-source";
    private static final int FILE_COUNT = 4;
    private static final Set<PosixFilePermission> SHARED_DIRECTORY = Set.of(
            PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
            PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE);
    private final Path warehouse;
    private final JdbcCatalog catalog;
    private final Table table;
    private final List<Path> dataFiles = new ArrayList<>();

    ProcessInputSnapshot(Path warehouse, String jdbcUrl) throws IOException {
        this.warehouse = warehouse;
        Files.createDirectories(warehouse);
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        configuration.setBoolean("fs.file.impl.disable.cache", true);
        catalog = (JdbcCatalog) CatalogUtil.buildIcebergCatalog("spec_catalog", Map.of(
                "type", "jdbc", "uri", jdbcUrl, "warehouse", warehouse.toString(),
                "jdbc.user", "pangrova", "jdbc.password", "pangrova_test", "jdbc.schema-version", "V1"), configuration);
        try {
            catalog.createNamespace(Namespace.of("default"));
            table = catalog.createTable(TableIdentifier.of("default", "input"), new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "payload", Types.StringType.get())), PartitionSpec.unpartitioned());
            appendActualRows();
            shareDirectories();
        } catch (IOException | RuntimeException | Error failure) {
            try { catalog.close(); } catch (RuntimeException | Error cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        }
    }

    Path warehouse() { return warehouse; }
    long snapshotId() { return table.currentSnapshot().snapshotId(); }
    List<Path> dataFiles() { return List.copyOf(dataFiles); }
    long committedRows(String tableName) {
        Table current = catalog.loadTable(TableIdentifier.of("default", tableName));
        current.refresh();
        return Long.parseLong(current.currentSnapshot().summary().get("total-records"));
    }

    private void appendActualRows() throws IOException {
        var append = table.newAppend();
        long rowsPerFile = ROW_COUNT / FILE_COUNT;
        for (int file = 0; file < FILE_COUNT; file++) {
            Path path = Path.of(table.location(), "data", "input-" + file + ".parquet");
            long from = Math.multiplyExact(file, rowsPerFile);
            long end = file == FILE_COUNT - 1 ? ROW_COUNT : Math.addExact(from, rowsPerFile);
            DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(path.toString()))
                    .schema(table.schema()).withSpec(table.spec()).createWriterFunc(GenericParquetWriter::create).build();
            try (writer) {
                GenericRecord row = GenericRecord.create(table.schema());
                row.setField("payload", PAYLOAD);
                for (long id = from; id < end; id++) {
                    row.setField("id", id);
                    writer.write(row);
                }
            }
            DataFile actual = writer.toDataFile();
            if (actual.recordCount() != end - from) throw new IllegalStateException("Actual footer row count mismatch");
            append.appendFile(actual);
            dataFiles.add(path);
        }
        append.commit();
        String committed = table.currentSnapshot().summary().get("total-records");
        if (!Long.toString(ROW_COUNT).equals(committed)) throw new IllegalStateException("Committed snapshot row count mismatch");
    }

    private void shareDirectories() throws IOException {
        try (var paths = Files.walk(warehouse)) {
            for (Path path : paths.filter(Files::isDirectory).toList()) Files.setPosixFilePermissions(path, SHARED_DIRECTORY);
        }
    }

    @Override public void close() throws IOException { catalog.close(); }
}
