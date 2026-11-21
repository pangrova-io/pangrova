package org.pangrova.connector.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Orphan removal had no tests. These pin the safety-critical behaviour: a file is
 * deleted only when it is both unreachable and older than the cutoff, and the
 * canonical-form matching used by {@code collectReachableFiles} keeps reachable
 * metadata alive.
 */
class IcebergOrphanFileRemoverTest {

    @TempDir
    Path warehouse;

    /** HadoopFileIO exposes prefix operations without declaring the interface. */
    private static final class PrefixFileIO implements SupportsPrefixOperations {
        private final HadoopFileIO delegate;

        PrefixFileIO(HadoopFileIO delegate) {
            this.delegate = delegate;
        }

        @Override public Iterable<FileInfo> listPrefix(String prefix) {
            return delegate.listPrefix(prefix);
        }

        @Override public void deletePrefix(String prefix) {
            delegate.deletePrefix(prefix);
        }

        @Override public InputFile newInputFile(String path) {
            return delegate.newInputFile(path);
        }

        @Override public OutputFile newOutputFile(String path) {
            return delegate.newOutputFile(path);
        }

        @Override public void deleteFile(String path) {
            delegate.deleteFile(path);
        }

        @Override public Map<String, String> properties() {
            return Map.of();
        }
    }

    private static Path localPath(String location) {
        return location.contains("://") ? Path.of(URI.create(location)) : Path.of(location);
    }

    private Table createTable(HadoopCatalog catalog, String name) {
        return catalog.createTable(
                TableIdentifier.of("db", name),
                new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
    }

    @Test
    void deletesOnlyUnreachableFilesOlderThanTheCutoff() throws Exception {
        HadoopTestGuard.assumeHadoopCompatible();
        var catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        Table table = createTable(catalog, "orphan_files");

        Path tableRoot = localPath(table.location());
        Path dataDir = tableRoot.resolve("data");
        Files.createDirectories(dataDir);
        Path orphan = dataDir.resolve("orphan" + FileFormat.PARQUET.toString().toLowerCase());
        Path recent = dataDir.resolve("recent" + FileFormat.PARQUET.toString().toLowerCase());
        Files.write(orphan, new byte[] {1, 2, 3});
        Files.write(recent, new byte[] {4});

        long cutoff = System.currentTimeMillis();
        Files.setLastModifiedTime(orphan, FileTime.fromMillis(cutoff - 60_000));
        Files.setLastModifiedTime(recent, FileTime.fromMillis(cutoff + 60_000));

        try (var hadoopIO = new HadoopFileIO(new Configuration())) {
            var io = new PrefixFileIO(hadoopIO);
            var result = IcebergOrphanFileRemover.sweep(table, io, cutoff, "db.orphan_files", false);
            assertThat(result.deletedCount()).isEqualTo(1);
            assertThat(result.scannedCount()).isGreaterThanOrEqualTo(2);
        }

        assertThat(Files.exists(orphan)).as("unreachable + old must be deleted").isFalse();
        assertThat(Files.exists(recent)).as("newer than cutoff must be kept").isTrue();
        // Metadata JSON is reachable and must survive even though it is old enough.
        assertThat(Files.exists(tableRoot.resolve("metadata"))).isTrue();
        try (var stream = Files.list(tableRoot.resolve("metadata"))) {
            assertThat(stream.count()).isPositive();
        }
    }

    @Test
    void dryRunReportsWithoutDeleting() throws Exception {
        HadoopTestGuard.assumeHadoopCompatible();
        var catalog = new HadoopCatalog(new Configuration(), warehouse.toString());
        Table table = createTable(catalog, "orphan_dry_run");

        Path dataDir = localPath(table.location()).resolve("data");
        Files.createDirectories(dataDir);
        Path orphan = dataDir.resolve("orphan.parquet");
        Files.write(orphan, new byte[] {9});
        long cutoff = System.currentTimeMillis();
        Files.setLastModifiedTime(orphan, FileTime.fromMillis(cutoff - 60_000));

        try (var hadoopIO = new HadoopFileIO(new Configuration())) {
            var result = IcebergOrphanFileRemover.sweep(
                    table, new PrefixFileIO(hadoopIO), cutoff, "db.orphan_dry_run", true);
            assertThat(result.orphanCount()).isEqualTo(1);
            assertThat(result.deletedCount()).isZero();
        }
        assertThat(Files.exists(orphan)).isTrue();
    }
}