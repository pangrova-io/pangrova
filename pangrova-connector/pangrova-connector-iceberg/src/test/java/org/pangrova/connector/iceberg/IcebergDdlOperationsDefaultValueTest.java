package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.handle.IcebergTableHandle;
import org.pangrova.connector.spi.data.type.PangrovaType;
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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for {@link IcebergDdlOperations#addColumn} with default values
 * using a real HadoopCatalog on local filesystem.
 */
class IcebergDdlOperationsDefaultValueTest {

    @TempDir
    Path warehouse;

    private Catalog catalog;

    private static final Schema BASE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    @BeforeEach
    void setUp() {
        HadoopTestGuard.assumeHadoopCompatible();
        catalog = createHadoopCatalog();
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            nsCatalog.createNamespace(Namespace.of("db"));
        }
        catalog.buildTable(TableIdentifier.of("db", "test_table"), BASE_SCHEMA)
                .withProperty("format-version", "3")
                .create();
    }

    @Test
    void addColumnWithStringDefault_setsDefaultInSchema() {
        IcebergTableHandle handle = buildHandle("db", "test_table");

        IcebergDdlOperations.addColumn(catalog, handle, "status", PangrovaType.VARCHAR, "active");

        Table table = catalog.loadTable(TableIdentifier.of("db", "test_table"));
        Types.NestedField statusField = table.schema().findField("status");
        assertThat(statusField).isNotNull();
        assertThat(statusField.type()).isEqualTo(Types.StringType.get());
        assertThat(statusField.initialDefault()).isNotNull();
    }

    @Test
    void addColumnWithUuidDefault_preservesTypedDefaultAfterReload() {
        UUID expected = UUID.fromString("80112233-4455-6677-8899-aabbccddeeff");
        IcebergTableHandle handle = buildHandle("db", "test_table");

        IcebergDdlOperations.addColumn(catalog, handle, "external_id", PangrovaType.UUID, expected);

        TableIdentifier name = TableIdentifier.of("db", "test_table");
        catalog.invalidateTable(name);
        Types.NestedField field = catalog.loadTable(name).schema().findField("external_id");
        assertThat(field.type()).isEqualTo(Types.UUIDType.get());
        assertThat(field.initialDefault()).isEqualTo(expected);
        assertThat(field.writeDefault()).isEqualTo(expected);
    }

    @Test
    void addColumnWithLongDefault_setsDefaultInSchema() {
        IcebergTableHandle handle = buildHandle("db", "test_table");

        IcebergDdlOperations.addColumn(catalog, handle, "count", PangrovaType.BIGINT, 42L);

        Table table = catalog.loadTable(TableIdentifier.of("db", "test_table"));
        Types.NestedField countField = table.schema().findField("count");
        assertThat(countField).isNotNull();
        assertThat(countField.type()).isEqualTo(Types.LongType.get());
        assertThat(countField.initialDefault()).isNotNull();
    }

    @Test
    void addColumnWithBooleanDefault_setsDefaultInSchema() {
        IcebergTableHandle handle = buildHandle("db", "test_table");

        IcebergDdlOperations.addColumn(catalog, handle, "active", PangrovaType.BOOLEAN, true);

        Table table = catalog.loadTable(TableIdentifier.of("db", "test_table"));
        Types.NestedField activeField = table.schema().findField("active");
        assertThat(activeField).isNotNull();
        assertThat(activeField.type()).isEqualTo(Types.BooleanType.get());
        assertThat(activeField.initialDefault()).isNotNull();
    }

    @Test
    void addColumnWithDoubleDefault_setsDefaultInSchema() {
        IcebergTableHandle handle = buildHandle("db", "test_table");

        IcebergDdlOperations.addColumn(catalog, handle, "score", PangrovaType.DOUBLE, 3.14);

        Table table = catalog.loadTable(TableIdentifier.of("db", "test_table"));
        Types.NestedField scoreField = table.schema().findField("score");
        assertThat(scoreField).isNotNull();
        assertThat(scoreField.type()).isEqualTo(Types.DoubleType.get());
        assertThat(scoreField.initialDefault()).isNotNull();
    }

    @Test
    void addColumnWithoutDefault_noDefaultInSchema() {
        IcebergTableHandle handle = buildHandle("db", "test_table");

        IcebergDdlOperations.addColumn(catalog, handle, "notes", PangrovaType.VARCHAR);

        Table table = catalog.loadTable(TableIdentifier.of("db", "test_table"));
        Types.NestedField notesField = table.schema().findField("notes");
        assertThat(notesField).isNotNull();
        assertThat(notesField.type()).isEqualTo(Types.StringType.get());
        assertThat(notesField.initialDefault()).isNull();
    }

    @Test
    void addColumnWithNullDefault_noDefaultInSchema() {
        IcebergTableHandle handle = buildHandle("db", "test_table");

        IcebergDdlOperations.addColumn(catalog, handle, "notes", PangrovaType.VARCHAR, null);

        Table table = catalog.loadTable(TableIdentifier.of("db", "test_table"));
        Types.NestedField notesField = table.schema().findField("notes");
        assertThat(notesField).isNotNull();
        assertThat(notesField.initialDefault()).isNull();
    }

    @Test
    void toIcebergLiteral_unsupportedType_throwsIllegalArgument() {
        assertThatThrownBy(() -> IcebergDdlOperations.toIcebergLiteral(new byte[]{1, 2, 3}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported default value type");
    }

    @Test
    void incompatibleDefault_doesNotAddColumn() {
        IcebergTableHandle handle = buildHandle("db", "test_table");

        assertThatThrownBy(() -> IcebergDdlOperations.addColumn(
                catalog, handle, "bad_col", PangrovaType.BIGINT, "not_a_number"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be cast");
        assertThat(catalog.loadTable(TableIdentifier.of("db", "test_table"))
                .schema().findField("bad_col")).isNull();
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
