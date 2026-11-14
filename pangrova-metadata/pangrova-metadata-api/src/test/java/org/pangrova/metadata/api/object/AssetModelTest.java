package org.pangrova.metadata.api.object;

import org.pangrova.metadata.api.enums.ViewSecurityMode;
import org.pangrova.metadata.api.enums.VolumeType;
import org.pangrova.metadata.api.enums.FunctionKind;
import org.pangrova.metadata.api.enums.AssetType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the extended asset model types introduced by G-META-07.
 */
class AssetModelTest {

    private static final Instant NOW = Instant.parse("2026-06-02T00:00:00Z");

    // ── ViewAsset ──

    @Test
    void viewAssetRoundTrip() {
        ViewAsset view = new ViewAsset(
                "cat", "db", "active_users",
                "SELECT * FROM users WHERE active = true",
                List.of(new ColumnMetadata("id", ColumnType.of("BIGINT"), false, "", Map.of()),
                        new ColumnMetadata("name", ColumnType.of("VARCHAR"), true, "", Map.of())),
                List.of(new QualifiedTableRef("cat", "db", "users")),
                ViewSecurityMode.INVOKER,
                "admin",
                Map.of("description", "active users"),
                NOW, NOW);

        assertThat(view.type()).isEqualTo(AssetType.VIEW);
        assertThat(view.catalog()).isEqualTo("cat");
        assertThat(view.schema()).isEqualTo("db");
        assertThat(view.name()).isEqualTo("active_users");
        assertThat(view.definitionSql()).contains("active = true");
        assertThat(view.columns()).hasSize(2);
        assertThat(view.dependentTables()).hasSize(1);
        assertThat(view.securityMode()).isEqualTo(ViewSecurityMode.INVOKER);
        assertThat(view.owner()).isEqualTo("admin");
    }

    @Test
    void viewAssetDefaultsNullSafety() {
        ViewAsset view = new ViewAsset(
                "c", "s", "v", "SELECT 1",
                null, null, null, null, null, NOW, NOW);

        assertThat(view.columns()).isEmpty();
        assertThat(view.dependentTables()).isEmpty();
        assertThat(view.securityMode()).isEqualTo(ViewSecurityMode.DEFINER);
        assertThat(view.owner()).isEmpty();
        assertThat(view.properties()).isEmpty();
    }

    // ── FunctionAsset ──

    @Test
    void functionAssetRoundTrip() {
        FunctionAsset fn = new FunctionAsset(
                "cat", "db", "double_it",
                FunctionKind.SCALAR,
                List.of(new FunctionAsset.Parameter("x", ColumnType.of("BIGINT"))),
                ColumnType.of("BIGINT"),
                "SQL",
                "RETURN x * 2",
                true,
                Map.of("cost", "low"),
                NOW, NOW);

        assertThat(fn.type()).isEqualTo(AssetType.FUNCTION);
        assertThat(fn.name()).isEqualTo("double_it");
        assertThat(fn.kind()).isEqualTo(FunctionKind.SCALAR);
        assertThat(fn.parameters()).hasSize(1);
        assertThat(fn.parameters().get(0).name()).isEqualTo("x");
        assertThat(fn.returnType().baseType()).isEqualTo("BIGINT");
        assertThat(fn.language()).isEqualTo("SQL");
        assertThat(fn.body()).isEqualTo("RETURN x * 2");
        assertThat(fn.deterministic()).isTrue();
    }

    @Test
    void functionAssetDefaultsNullSafety() {
        FunctionAsset fn = new FunctionAsset(
                "c", "s", "f", null, null, null,
                null, null, false, null, NOW, NOW);

        assertThat(fn.kind()).isEqualTo(FunctionKind.SCALAR);
        assertThat(fn.parameters()).isEmpty();
        assertThat(fn.language()).isEmpty();
        assertThat(fn.body()).isEmpty();
        assertThat(fn.properties()).isEmpty();
    }

    // ── VolumeAsset ──

    @Test
    void volumeAssetRoundTrip() {
        VolumeAsset vol = new VolumeAsset(
                "cat", "db", "raw_data",
                VolumeType.EXTERNAL,
                "s3://bucket/raw/",
                Map.of("description", "raw data files"),
                NOW, NOW);

        assertThat(vol.type()).isEqualTo(AssetType.VOLUME);
        assertThat(vol.catalog()).isEqualTo("cat");
        assertThat(vol.schema()).isEqualTo("db");
        assertThat(vol.name()).isEqualTo("raw_data");
        assertThat(vol.volumeType()).isEqualTo(VolumeType.EXTERNAL);
        assertThat(vol.storageLocation()).isEqualTo("s3://bucket/raw/");
    }

    @Test
    void volumeAssetDefaultsNullSafety() {
        VolumeAsset vol = new VolumeAsset(
                "c", "s", "v", null, null, null, NOW, NOW);

        assertThat(vol.volumeType()).isEqualTo(VolumeType.MANAGED);
        assertThat(vol.storageLocation()).isEmpty();
        assertThat(vol.properties()).isEmpty();
    }

    @Test
    void volumeAssetRejectsNullCatalog() {
        assertThatThrownBy(() -> new VolumeAsset(
                null, "s", "v", null, null, null, NOW, NOW))
                .isInstanceOf(NullPointerException.class);
    }

    // ── ModelAsset ──

    @Test
    void modelAssetRoundTrip() {
        ModelAsset model = new ModelAsset(
                "cat", "db", "iris_classifier",
                "v3", "ONNX", "s3://models/iris/v3/model.onnx",
                List.of(new ColumnMetadata("sepal_length", ColumnType.of("DOUBLE"), false, "", Map.of())),
                List.of(new ColumnMetadata("species", ColumnType.of("VARCHAR"), false, "", Map.of())),
                List.of("production"),
                Map.of("accuracy", "0.97"),
                NOW, NOW);

        assertThat(model.type()).isEqualTo(AssetType.MODEL);
        assertThat(model.version()).isEqualTo("v3");
        assertThat(model.framework()).isEqualTo("ONNX");
    }

    // ── MaterializedViewAsset ──

    @Test
    void materializedViewAssetRoundTrip() {
        MaterializedViewAsset mv = new MaterializedViewAsset(
                "cat", "db", "daily_sales",
                "SELECT date, SUM(amount) FROM sales GROUP BY date",
                List.of(new ColumnMetadata("date", ColumnType.of("DATE"), false, "", Map.of()),
                        new ColumnMetadata("total", ColumnType.of("DECIMAL"), false, "", Map.of())),
                List.of(new QualifiedTableRef("cat", "db", "sales")),
                "ON_SCHEDULE",
                "s3://warehouse/mv/daily_sales/",
                NOW,
                Map.of(),
                NOW, NOW);

        assertThat(mv.type()).isEqualTo(AssetType.MATERIALIZED_VIEW);
        assertThat(mv.refreshStrategy()).isEqualTo("ON_SCHEDULE");
    }

    // ── TableAsset ──

    @Test
    void tableAssetFromTableMetadata() {
        var tm = new org.pangrova.metadata.api.object.TableMetadata(
                "cat", "db", "orders",
                List.of(new ColumnMetadata("id", ColumnType.of("BIGINT"), false, "", Map.of())),
                Map.of("year", "identity(order_date)"),
                List.of("id"),
                Map.of("format", "parquet"),
                NOW, NOW);

        TableAsset ta = TableAsset.from(tm);
        assertThat(ta.type()).isEqualTo(AssetType.TABLE);
        assertThat(ta.catalog()).isEqualTo("cat");
        assertThat(ta.toTableMetadata().table()).isEqualTo("orders");
    }

    // ── AssetType coverage ──

    @Test
    void assetTypeEnumCoversAllExpectedKinds() {
        assertThat(AssetType.values()).containsExactly(
                AssetType.TABLE,
                AssetType.VIEW,
                AssetType.MATERIALIZED_VIEW,
                AssetType.FUNCTION,
                AssetType.MODEL,
                AssetType.VOLUME);
    }

    // ── Sealed interface completeness ──

    @Test
    void allAssetTypesHaveImplementations() {
        // Verify each AssetType has at least one implementation in the sealed hierarchy
        Asset table = new TableAsset("c", "s", "t", List.of(), Map.of(), List.of(), Map.of(), NOW, NOW);
        Asset view = new ViewAsset("c", "s", "v", "SQL", null, null, null, null, null, NOW, NOW);
        Asset mv = new MaterializedViewAsset("c", "s", "mv", "SQL", null, null, null, null, null, null, NOW, NOW);
        Asset fn = new FunctionAsset("c", "s", "f", null, null, null, null, null, false, null, NOW, NOW);
        Asset model = new ModelAsset("c", "s", "m", "v1", null, null, null, null, null, null, NOW, NOW);
        Asset volume = new VolumeAsset("c", "s", "vol", null, null, null, NOW, NOW);

        assertThat(table.type()).isEqualTo(AssetType.TABLE);
        assertThat(view.type()).isEqualTo(AssetType.VIEW);
        assertThat(mv.type()).isEqualTo(AssetType.MATERIALIZED_VIEW);
        assertThat(fn.type()).isEqualTo(AssetType.FUNCTION);
        assertThat(model.type()).isEqualTo(AssetType.MODEL);
        assertThat(volume.type()).isEqualTo(AssetType.VOLUME);
    }
}
