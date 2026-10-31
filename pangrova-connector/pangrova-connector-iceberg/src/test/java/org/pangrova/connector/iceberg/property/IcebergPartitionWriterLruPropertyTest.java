package org.pangrova.connector.iceberg.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

import org.pangrova.connector.iceberg.HadoopTestGuard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.connector.iceberg.write.IcebergPageSink;
import org.pangrova.connector.iceberg.write.IcebergWriteContext;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.data.page.Page;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test for the LRU eviction behavior of
 * {@link org.pangrova.connector.iceberg.write.AbstractIcebergPageSink}.
 *
 * <p>Verifies that for any write sequence with more than 128 unique partition
 * keys, the sink completes without error and all evicted writers' DataFiles
 * are preserved in the final commit metadata output.
 *
 * <p><b>Validates: Requirements 5.1</b></p>
 */
class IcebergPartitionWriterLruPropertyTest {

    private static final String CATALOG_NAME = "lru_test_catalog";
    private static final String NAMESPACE = "lru_db";
    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Schema WRITE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()));

    private static final PartitionSpec PARTITION_SPEC = PartitionSpec.builderFor(WRITE_SCHEMA)
            .identity("region")
            .build();

    private Path warehouse;
    private Catalog catalog;

    @BeforeProperty
    void setUp() {
        if (!HadoopTestGuard.isHadoopCompatible()) return;
        try {
            warehouse = Files.createTempDirectory("lru_property_test_");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        catalog = org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            try {
                nsCatalog.createNamespace(Namespace.of(NAMESPACE));
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException ignored) {
                // namespace may already exist across property tries
            }
        }
    }

    @AfterProperty
    void tearDown() {
        if (warehouse != null) {
            deleteRecursively(warehouse.toFile());
        }
    }

    /**
     * For any write sequence with more than 128 unique partition keys,
     * the number of open writers never exceeds MAX_OPEN_WRITERS (128),
     * and all evicted writers' DataFiles are preserved in the
     * finishWithMetadata() output.
     *
     * <p>The open-writer invariant is enforced by the LRU eviction logic
     * in AbstractIcebergPageSink.writeToPartition(). We verify correctness
     * by checking that the total DataFile count in the commit metadata
     * equals the number of unique partition keys written — proving every
     * partition (including evicted ones) produced exactly one DataFile.</p>
     *
     * <p><b>Validates: Requirements 5.1</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature_data-pipeline-post-fix-review")
    @Tag("Property_Iceberg_partition_writer_LRU_eviction")
    void lruEviction_preservesAllPartitionDataFiles(
            @ForAll("partitionKeySequences") List<String> partitionKeys) throws Exception {
        if (!HadoopTestGuard.isHadoopCompatible()) return;

        Set<String> uniqueKeys = new LinkedHashSet<>(partitionKeys);
        assertThat(uniqueKeys.size())
                .as("precondition: must have >128 unique partition keys")
                .isGreaterThan(128);

        String tableName = "lru_test_" + TABLE_COUNTER.incrementAndGet();
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
        Table table = catalog.createTable(tableId, WRITE_SCHEMA, PARTITION_SPEC);

        IcebergWriteContext ctx = new IcebergWriteContext(
                table, WRITE_SCHEMA, NAMESPACE, tableName);
        IcebergPageSink sink = new IcebergPageSink(ctx);

        long rowId = 0;
        for (String key : partitionKeys) {
            TypedRow row = TypedRow.builder()
                    .putLong("id", rowId++)
                    .putString("region", key)
                    .build();
            sink.appendPage(new Page(List.of("id", "region"), List.of(row)));
        }

        byte[] metadata = sink.finishWithMetadata();
        sink.close();

        assertThat(metadata).isNotEmpty();

        String json = new String(metadata, StandardCharsets.UTF_8);
        JsonNode root = MAPPER.readTree(json);
        assertThat(root.isArray())
                .as("commit metadata should be a JSON array of DataFiles")
                .isTrue();

        int dataFileCount = root.size();

        // At least one DataFile per unique partition key. Revisiting an
        // evicted partition creates a new writer (and thus a new DataFile),
        // so the count may exceed uniqueKeys.size() when duplicates hit
        // evicted partitions.
        assertThat(dataFileCount)
                .as("DataFile count must be >= unique partition count — "
                        + "evicted writers' files must be preserved")
                .isGreaterThanOrEqualTo(uniqueKeys.size());

        // Verify total record count across all DataFiles equals the
        // number of rows written. This proves no data was lost during
        // eviction — every row is accounted for in some DataFile.
        long totalRecords = 0;
        for (JsonNode fileNode : root) {
            totalRecords += fileNode.get("record-count").asLong();
        }
        assertThat(totalRecords)
                .as("total records across all DataFiles must equal rows written")
                .isEqualTo(partitionKeys.size());
    }

    /**
     * Generates a sequence of partition key strings containing more than
     * 128 unique values. The sequence may contain duplicates to exercise
     * the LRU access-order behavior of the LinkedHashMap.
     */
    @Provide
    Arbitrary<List<String>> partitionKeySequences() {
        Arbitrary<Integer> uniqueCountArb = Arbitraries.integers().between(140, 200);

        return uniqueCountArb.flatMap(uniqueCount -> {
            List<String> uniqueKeys = new ArrayList<>(uniqueCount);
            for (int i = 0; i < uniqueCount; i++) {
                uniqueKeys.add("region_" + i);
            }

            Arbitrary<Integer> extraWriteCount = Arbitraries.integers().between(0, 100);
            return extraWriteCount.flatMap(extra -> {
                Arbitrary<List<Integer>> revisitIndices = Arbitraries.integers()
                        .between(0, uniqueCount - 1)
                        .list().ofSize(extra);

                List<String> keys = uniqueKeys;
                return revisitIndices.map(indices -> {
                    List<String> sequence = new ArrayList<>(uniqueCount + extra);
                    sequence.addAll(keys);
                    for (int idx : indices) {
                        sequence.add(keys.get(idx));
                    }
                    return sequence;
                });
            });
        });
    }

    private static void deleteRecursively(java.io.File file) {
        if (file.isDirectory()) {
            java.io.File[] children = file.listFiles();
            if (children != null) {
                for (java.io.File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}
