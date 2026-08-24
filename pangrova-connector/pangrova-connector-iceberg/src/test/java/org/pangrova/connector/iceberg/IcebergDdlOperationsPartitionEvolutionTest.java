package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link IcebergDdlOperations#setPartitionSpec} verifying
 * partition evolution via Iceberg's {@code UpdatePartitionSpec} API (G-CONN-08).
 *
 * <p>Iceberg partition evolution allows adding and removing partition fields
 * without rewriting existing data files. Old data files retain their original
 * spec ID, while new writes use the evolved spec.</p>
 */
class IcebergDdlOperationsPartitionEvolutionTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;

    private static final Schema BASE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(4, "region", Types.StringType.get()),
            Types.NestedField.optional(5, "amount", Types.DoubleType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = createHadoopCatalog();
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of("db"));
        }
    }

    @Test
    void addIdentityPartitionToUnpartitionedTable() {
        catalog.buildTable(TableIdentifier.of("db", "t1"), BASE_SCHEMA)
                .withProperty("format-version", "2")
                .create();
        IcebergTableHandle handle = buildHandle("db", "t1");

        List<Map.Entry<String, List<String>>> transforms = List.of(
                identity("region"));

        IcebergDdlOperations.setPartitionSpec(catalog, handle, transforms);

        Table table = catalog.loadTable(TableIdentifier.of("db", "t1"));
        PartitionSpec spec = table.spec();
        assertThat(spec.fields()).hasSize(1);
        assertThat(spec.fields().get(0).name()).isEqualTo("region");
        assertThat(spec.fields().get(0).transform().toString()).isEqualTo("identity");
    }

    @Test
    void addYearPartitionTransform() {
        catalog.buildTable(TableIdentifier.of("db", "t2"), BASE_SCHEMA)
                .withProperty("format-version", "2")
                .create();
        IcebergTableHandle handle = buildHandle("db", "t2");

        List<Map.Entry<String, List<String>>> transforms = List.of(
                transform("year", List.of("ts")));

        IcebergDdlOperations.setPartitionSpec(catalog, handle, transforms);

        Table table = catalog.loadTable(TableIdentifier.of("db", "t2"));
        PartitionSpec spec = table.spec();
        assertThat(spec.fields()).hasSize(1);
        assertThat(spec.fields().get(0).transform().toString()).isEqualTo("year");
    }

    @Test
    void addBucketPartitionTransform() {
        catalog.buildTable(TableIdentifier.of("db", "t3"), BASE_SCHEMA)
                .withProperty("format-version", "2")
                .create();
        IcebergTableHandle handle = buildHandle("db", "t3");

        List<Map.Entry<String, List<String>>> transforms = List.of(
                transform("bucket", List.of("id", "16")));

        IcebergDdlOperations.setPartitionSpec(catalog, handle, transforms);

        Table table = catalog.loadTable(TableIdentifier.of("db", "t3"));
        PartitionSpec spec = table.spec();
        assertThat(spec.fields()).hasSize(1);
        assertThat(spec.fields().get(0).transform().toString()).isEqualTo("bucket[16]");
    }

    @Test
    void evolvePartitionSpecFromIdentityToMonthly() {
        // Start with identity(region) partition
        catalog.buildTable(TableIdentifier.of("db", "t4"), BASE_SCHEMA)
                .withPartitionSpec(PartitionSpec.builderFor(BASE_SCHEMA)
                        .identity("region")
                        .build())
                .withProperty("format-version", "2")
                .create();
        IcebergTableHandle handle = buildHandle("db", "t4");

        // Evolve to month(ts) -- this should drop region and add month(ts)
        List<Map.Entry<String, List<String>>> transforms = List.of(
                transform("month", List.of("ts")));

        IcebergDdlOperations.setPartitionSpec(catalog, handle, transforms);

        Table table = catalog.loadTable(TableIdentifier.of("db", "t4"));
        PartitionSpec spec = table.spec();
        // Only month(ts) should be active; region should be void-transformed
        List<PartitionField> activeFields = spec.fields().stream()
                .filter(f -> !f.transform().toString().equals("void"))
                .toList();
        assertThat(activeFields).hasSize(1);
        assertThat(activeFields.get(0).transform().toString()).isEqualTo("month");
    }

    @Test
    void addMultiplePartitionFields() {
        catalog.buildTable(TableIdentifier.of("db", "t5"), BASE_SCHEMA)
                .withProperty("format-version", "2")
                .create();
        IcebergTableHandle handle = buildHandle("db", "t5");

        List<Map.Entry<String, List<String>>> transforms = List.of(
                identity("region"),
                transform("day", List.of("ts")));

        IcebergDdlOperations.setPartitionSpec(catalog, handle, transforms);

        Table table = catalog.loadTable(TableIdentifier.of("db", "t5"));
        PartitionSpec spec = table.spec();
        assertThat(spec.fields()).hasSize(2);
    }

    @Test
    void removeAllPartitions_convertsToUnpartitioned() {
        // Start with partitioned table
        catalog.buildTable(TableIdentifier.of("db", "t6"), BASE_SCHEMA)
                .withPartitionSpec(PartitionSpec.builderFor(BASE_SCHEMA)
                        .identity("region")
                        .build())
                .withProperty("format-version", "2")
                .create();
        IcebergTableHandle handle = buildHandle("db", "t6");

        // Pass empty transforms to make it unpartitioned
        IcebergDdlOperations.setPartitionSpec(catalog, handle, List.of());

        Table table = catalog.loadTable(TableIdentifier.of("db", "t6"));
        PartitionSpec spec = table.spec();
        // All fields should be void-transformed (Iceberg keeps history)
        List<PartitionField> activeFields = spec.fields().stream()
                .filter(f -> !f.transform().toString().equals("void"))
                .toList();
        assertThat(activeFields).isEmpty();
    }

    @Test
    void noOpWhenSpecMatchesCurrent() {
        // Create with identity(region)
        catalog.buildTable(TableIdentifier.of("db", "t7"), BASE_SCHEMA)
                .withPartitionSpec(PartitionSpec.builderFor(BASE_SCHEMA)
                        .identity("region")
                        .build())
                .withProperty("format-version", "2")
                .create();

        Table before = catalog.loadTable(TableIdentifier.of("db", "t7"));
        int specIdBefore = before.spec().specId();

        IcebergTableHandle handle = buildHandle("db", "t7");
        // Request the same spec -- should be a no-op
        List<Map.Entry<String, List<String>>> transforms = List.of(
                identity("region"));

        IcebergDdlOperations.setPartitionSpec(catalog, handle, transforms);

        Table after = catalog.loadTable(TableIdentifier.of("db", "t7"));
        // Spec ID should not change since nothing was modified
        assertThat(after.spec().specId()).isEqualTo(specIdBefore);
    }

    @Test
    void truncatePartitionTransform() {
        catalog.buildTable(TableIdentifier.of("db", "t8"), BASE_SCHEMA)
                .withProperty("format-version", "2")
                .create();
        IcebergTableHandle handle = buildHandle("db", "t8");

        List<Map.Entry<String, List<String>>> transforms = List.of(
                transform("truncate", List.of("name", "5")));

        IcebergDdlOperations.setPartitionSpec(catalog, handle, transforms);

        Table table = catalog.loadTable(TableIdentifier.of("db", "t8"));
        PartitionSpec spec = table.spec();
        assertThat(spec.fields()).hasSize(1);
        assertThat(spec.fields().get(0).transform().toString()).isEqualTo("truncate[5]");
    }

    @Test
    void hourPartitionTransform() {
        catalog.buildTable(TableIdentifier.of("db", "t9"), BASE_SCHEMA)
                .withProperty("format-version", "2")
                .create();
        IcebergTableHandle handle = buildHandle("db", "t9");

        List<Map.Entry<String, List<String>>> transforms = List.of(
                transform("hour", List.of("ts")));

        IcebergDdlOperations.setPartitionSpec(catalog, handle, transforms);

        Table table = catalog.loadTable(TableIdentifier.of("db", "t9"));
        PartitionSpec spec = table.spec();
        assertThat(spec.fields()).hasSize(1);
        assertThat(spec.fields().get(0).transform().toString()).isEqualTo("hour");
    }

    @Test
    void unsupportedTransformThrows() {
        catalog.buildTable(TableIdentifier.of("db", "t10"), BASE_SCHEMA)
                .withProperty("format-version", "2")
                .create();
        IcebergTableHandle handle = buildHandle("db", "t10");

        List<Map.Entry<String, List<String>>> transforms = List.of(
                transform("unsupported_transform", List.of("name")));

        assertThatThrownBy(() ->
                IcebergDdlOperations.setPartitionSpec(catalog, handle, transforms))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported partition transform");
    }

    // ── Helpers ──

    private static Map.Entry<String, List<String>> identity(String column) {
        return new AbstractMap.SimpleImmutableEntry<>(null, List.of(column));
    }

    private static Map.Entry<String, List<String>> transform(String funcName, List<String> args) {
        return new AbstractMap.SimpleImmutableEntry<>(funcName, args);
    }

    private Catalog createHadoopCatalog() {
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        return org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                "test_catalog", props, new org.apache.hadoop.conf.Configuration());
    }

    private IcebergTableHandle buildHandle(String schema, String table) {
        Table icebergTable = catalog.loadTable(TableIdentifier.of(schema, table));
        return new IcebergTableHandle(
                schema, table,
                icebergTable.location(),
                Optional.empty(),
                icebergTable.schema().toString(),
                icebergTable.spec().toString(),
                2,
                icebergTable.properties(),
                IcebergTableHandle.READ_MODE_FULL,
                0L, 0L, IcebergTableHandle.NO_LIMIT);
    }
}
