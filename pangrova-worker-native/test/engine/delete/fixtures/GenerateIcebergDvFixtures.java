package org.apache.iceberg.deletes;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.StandardBlobTypes;

/** Reproduce Native regression fixtures using the pinned, unmodified Iceberg SDK. */
public final class GenerateIcebergDvFixtures {
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        write(root, "basic", 1, 5, 100, 9999);
        long[] large = java.util.stream.LongStream.iterate(0, n -> n < 10000, n -> n + 3).toArray();
        write(root, "large", large);
        write(root, "factory-basic", 1, 3, 7);
        write(root, "factory-first", 0, 2);
        write(root, "factory-second", 4, 6);
        write(root, "factory-zero", 0);
        write(root, "cross", 10, 20, 30, 500, 65535);
        // BaseDVFileWriter deliberately omits empty outputs. Use the same SDK
        // bitmap serializer and Puffin writer to exercise an empty vector.
        try (var writer = Puffin.write(Files.localOutput(root.resolve("iceberg-sdk-empty-dv.puffin").toString())).build()) {
            writer.write(new Blob(StandardBlobTypes.DV_V1, List.of(MetadataColumns.ROW_POSITION.fieldId()),
                    -1, -1, new BitmapPositionDeleteIndex().serialize(), null,
                    Map.of("referenced-data-file", "data.parquet", "cardinality", "0")));
        }
        try (var writer = new BaseDVFileWriter(
                () -> Files.localOutput(root.resolve("iceberg-sdk-multiple-dv.puffin").toString()), ignored -> null)) {
            writer.delete("s3://fixture/a.parquet", 1, PartitionSpec.unpartitioned(), null);
            writer.delete("s3://fixture/b.parquet", 2, PartitionSpec.unpartitioned(), null);
            writer.delete("s3://fixture/b.parquet", (1L << 32) + 7, PartitionSpec.unpartitioned(), null);
        }
    }

    private static void write(Path root, String name, long... positions) throws Exception {
        try (var writer = new BaseDVFileWriter(
                () -> Files.localOutput(root.resolve("iceberg-sdk-" + name + "-dv.puffin").toString()), ignored -> null)) {
            for (long position : positions) writer.delete("data.parquet", position, PartitionSpec.unpartitioned(), null);
        }
    }
}
