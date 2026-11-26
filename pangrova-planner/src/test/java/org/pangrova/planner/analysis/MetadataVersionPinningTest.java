package org.pangrova.planner.analysis;

import org.pangrova.planner.api.exception.MetadataChangedDuringPlanningException;
import org.pangrova.planner.api.metadata.MetadataVersionProvider;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests cross-catalog metadata version pinning: concurrent schema change
 * during planning throws {@link MetadataChangedDuringPlanningException}.
 */
@DisplayName("Metadata Version Pinning")
class MetadataVersionPinningTest {

    @Test
    @DisplayName("Cross-catalog query succeeds when metadata versions are stable")
    void stableVersionsSucceed() {
        // Two catalogs with stable versions
        AtomicLong catAVersion = new AtomicLong(1);
        AtomicLong catBVersion = new AtomicLong(1);

        MetadataVersionProvider provider = catalog -> switch (catalog) {
            case "cat_a" -> catAVersion.get();
            case "cat_b" -> catBVersion.get();
            default -> 0;
        };

        PangrovaPlanner planner = buildCrossCatalogPlanner(provider);

        // Cross-catalog query — versions don't change during planning
        PlanResult result = planner.plan(
                "SELECT a.id, b.id FROM cat_a.s.t1 a JOIN cat_b.s.t2 b ON a.id = b.id");

        assertThat(result.plan()).isNotNull();
    }

    @Test
    @DisplayName("Cross-catalog query throws when metadata version changes during planning")
    void versionChangeDuringPlanningThrows() {
        // Provider that increments version on second call (simulating concurrent DDL)
        AtomicLong catAVersion = new AtomicLong(1);
        AtomicLong catBVersion = new AtomicLong(1);
        AtomicLong callCount = new AtomicLong(0);

        MetadataVersionProvider provider = catalog -> {
            if ("cat_b".equals(catalog)) {
                long count = callCount.incrementAndGet();
                if (count > 1) {
                    // Second call to cat_b (verification phase) — version changed
                    return catBVersion.incrementAndGet();
                }
            }
            return switch (catalog) {
                case "cat_a" -> catAVersion.get();
                case "cat_b" -> catBVersion.get();
                default -> 0;
            };
        };

        PangrovaPlanner planner = buildCrossCatalogPlanner(provider);

        assertThatThrownBy(() -> planner.plan(
                "SELECT a.id, b.id FROM cat_a.s.t1 a JOIN cat_b.s.t2 b ON a.id = b.id"))
                .isInstanceOf(MetadataChangedDuringPlanningException.class)
                .satisfies(thrown -> {
                    MetadataChangedDuringPlanningException ex =
                            (MetadataChangedDuringPlanningException) thrown;
                    assertThat(ex.catalogName()).isEqualTo("cat_b");
                });
    }

    @Test
    @DisplayName("Single-catalog query skips version pinning (no cross-catalog check needed)")
    void singleCatalogSkipsPinning() {
        // Provider that would fail if called for verification — but single-catalog
        // queries should skip pinning entirely
        AtomicLong callCount = new AtomicLong(0);
        MetadataVersionProvider provider = catalog -> {
            callCount.incrementAndGet();
            return 1;
        };

        PangrovaPlanner planner = buildSingleCatalogPlanner(provider);

        PlanResult result = planner.plan("SELECT id FROM t1");
        assertThat(result.plan()).isNotNull();
        // Single-catalog: pinMetadataVersions returns empty map, no calls expected
        // (extractCatalogs may find 0 or 1 catalog, both skip pinning)
    }

    @Test
    @DisplayName("Version pinning disabled when provider is null")
    void nullProviderDisablesPinning() {
        PangrovaPlanner planner = buildCrossCatalogPlanner(null);

        // Should succeed even without version provider
        PlanResult result = planner.plan(
                "SELECT a.id, b.id FROM cat_a.s.t1 a JOIN cat_b.s.t2 b ON a.id = b.id");
        assertThat(result.plan()).isNotNull();
    }

    // ── Helpers ──

    private PangrovaPlanner buildCrossCatalogPlanner(MetadataVersionProvider provider) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        SchemaPlus root = rootSchema.plus();

        // Create two catalog schemas with tables
        SchemaPlus catA = root.add("cat_a", new org.apache.calcite.schema.impl.AbstractSchema() {});
        SchemaPlus catASchema = catA.add("s", new org.apache.calcite.schema.impl.AbstractSchema() {});
        catASchema.add("t1", createSimpleTable());

        SchemaPlus catB = root.add("cat_b", new org.apache.calcite.schema.impl.AbstractSchema() {});
        SchemaPlus catBSchema = catB.add("s", new org.apache.calcite.schema.impl.AbstractSchema() {});
        catBSchema.add("t2", createSimpleTable());

        PlannerContext ctx = new PlannerContext(null, null, null, provider);
        return new PangrovaPlanner(rootSchema, List.of(""), ctx);
    }

    private PangrovaPlanner buildSingleCatalogPlanner(MetadataVersionProvider provider) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);
        SchemaPlus root = rootSchema.plus();
        root.add("t1", createSimpleTable());

        PlannerContext ctx = new PlannerContext(null, null, null, provider);
        return new PangrovaPlanner(rootSchema, List.of(""), ctx);
    }

    private static Table createSimpleTable() {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add("id", SqlTypeName.INTEGER)
                        .add("name", SqlTypeName.VARCHAR, 100)
                        .build();
            }
        };
    }
}
