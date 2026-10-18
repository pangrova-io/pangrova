package org.pangrova.planner.mv;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.parser.ast.RefreshStrategy;
import org.pangrova.planner.parse.PangrovaParserFactory;
import org.pangrova.planner.rewrite.DynamicTableSqlRewriter;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.security.AccessDeniedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests Dynamic Table query transparency rewrite via {@link DynamicTableRewriter}.
 *
 * Validates: Requirements 12
 */
class DynamicTableQueryRewriteTest {

    private MaterializedViewRegistry mvManager;
    private DynamicTableRewriter rewriter;

    @BeforeEach
    void setUp() {
        mvManager = new MaterializedViewRegistry();
        rewriter = new DynamicTableRewriter(mvManager);
    }

    // --- ACTIVE DT: enableQueryRewrite=true, INCREMENTAL → rewrite success ---

    @Test
    void activeDtRewritesToTargetTable() {
        registerDt("sales_summary", RefreshStrategy.INCREMENTAL, true);

        Optional<String> target = rewriter.resolveTargetTable("sales_summary");

        assertThat(target).isPresent().hasValue("__dt_sales_summary");
    }

    // --- CREATING DT: enableQueryRewrite=false, INCREMENTAL → error ---

    @Test
    void creatingDtThrowsNotReady() {
        registerDt("pending_dt", RefreshStrategy.INCREMENTAL, false);

        assertThatThrownBy(() -> rewriter.resolveTargetTable("pending_dt"))
                .isInstanceOf(DynamicTableNotReadyException.class)
                .satisfies(e -> {
                    DynamicTableNotReadyException ex = (DynamicTableNotReadyException) e;
                    assertThat(ex.dtName()).isEqualTo("pending_dt");
                    assertThat(ex.getErrorCode()).isEqualTo(PangrovaErrorCode.DYNAMIC_TABLE_NOT_READY);
                    assertThat(ex.getMessage()).contains("not ready");
                });
    }

    // --- DROPPED DT: definition not found → pass through (empty) ---

    @Test
    void droppedDtPassesThrough() {
        // No registration — simulates a dropped or non-existent DT
        Optional<String> target = rewriter.resolveTargetTable("dropped_dt");

        assertThat(target).isEmpty();
    }

    // --- SUSPENDED DT: enableQueryRewrite=true, INCREMENTAL → queryable ---

    @Test
    void suspendedDtWithQueryRewriteEnabledReturnsTarget() {
        // SUSPENDED state still has enableQueryRewrite=true (data is stale but queryable)
        registerDt("suspended_dt", RefreshStrategy.INCREMENTAL, true);

        Optional<String> target = rewriter.resolveTargetTable("suspended_dt");

        assertThat(target).isPresent().hasValue("__dt_suspended_dt");
    }

    // --- Non-DT table: ON_SCHEDULE MV → not affected ---

    @Test
    void onScheduleMvNotAffected_viaRegister() {
        registerDt("regular_mv", RefreshStrategy.ON_SCHEDULE, true);

        Optional<String> target = rewriter.resolveTargetTable("regular_mv");

        assertThat(target).isEmpty();
    }

    // --- Non-DT table: ON_SCHEDULE MV → not affected ---

    @Test
    void onScheduleMvNotAffected() {
        registerDt("scheduled_mv", RefreshStrategy.ON_SCHEDULE, true);

        Optional<String> target = rewriter.resolveTargetTable("scheduled_mv");

        assertThat(target).isEmpty();
    }

    // --- Unknown table: not registered at all → pass through ---

    @Test
    void unknownTablePassesThrough() {
        Optional<String> target = rewriter.resolveTargetTable("unknown_table");

        assertThat(target).isEmpty();
    }

    // --- Target prefix is correct ---

    @Test
    void targetPrefixIsConsistent() {
        assertThat(DynamicTableRewriter.TARGET_PREFIX).isEqualTo("__dt_");
    }

    @Test
    void selfJoinPinsOnePublishedGenerationBeforeSecondReference() {
        var logical = new QualifiedTableName("lake", "analytics", "rollup");
        var physical = new QualifiedTableName("lake", "analytics", "__pg_dt_first");
        var reads = new AtomicInteger();
        DynamicTableRewriter.PublishedTargetResolver resolver =
                new DynamicTableRewriter.PublishedTargetResolver() {
                    @Override
                    public Optional<QualifiedTableName> canonicalLogical(List<String> nameParts) {
                        return Optional.of(logical);
                    }

                    @Override
                    public Optional<DynamicTableRewriter.PublishedTarget> resolve(List<String> nameParts) {
                        if (reads.incrementAndGet() != 1) {
                            throw new IllegalStateException("Publication changed during one SQL rewrite");
                        }
                        return Optional.of(new DynamicTableRewriter.PublishedTarget(logical, physical));
                    }
                };
        var sqlRewriter = new DynamicTableSqlRewriter(
                new PangrovaParserFactory(), new DynamicTableRewriter(resolver));

        var rewritten = sqlRewriter.rewriteWithAuthority("SELECT a.id FROM lake.analytics.rollup a "
                + "JOIN lake.analytics.rollup b ON a.id = b.id");

        assertThat(reads).hasValue(1);
        assertThat(rewritten.sql()).contains("FROM lake.analytics.__pg_dt_first a",
                "JOIN lake.analytics.__pg_dt_first b");
        assertThat(rewritten.physicalToLogical()).containsEntry(physical, logical);
    }

    @Test
    void explicitPhysicalGenerationInSameSqlCannotInheritLogicalAuthority() {
        var logical = new QualifiedTableName("lake", "analytics", "rollup");
        var physical = new QualifiedTableName("lake", "analytics", "__pg_dt_first");
        var sqlRewriter = new DynamicTableSqlRewriter(new PangrovaParserFactory(),
                new DynamicTableRewriter(parts -> Optional.of(
                        new DynamicTableRewriter.PublishedTarget(logical, physical))));

        for (String sql : List.of(
                "SELECT a.id FROM lake.analytics.rollup a JOIN lake.analytics.__pg_dt_first b ON a.id=b.id",
                "SELECT a.id FROM lake.analytics.rollup a, lake.analytics.__pg_dt_first b",
                "WITH c AS (SELECT id FROM lake.analytics.__pg_dt_first) "
                        + "SELECT id FROM lake.analytics.rollup")) {
            assertThatThrownBy(() -> sqlRewriter.rewriteWithAuthority(sql))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("Direct access");
        }
        assertThat(sqlRewriter.rewriteWithAuthority(
                "SELECT id AS __pg_dt_alias FROM lake.analytics.rollup").sql())
                .contains("lake.analytics.__pg_dt_first");
    }

    // --- helper ---

    private void registerDt(String name, RefreshStrategy strategy, boolean enableQueryRewrite) {
        MaterializedViewDefinition def = new MaterializedViewDefinition(
                name,
                "SELECT * FROM source",
                strategy,
                enableQueryRewrite,
                Optional.of(Instant.now()),
                Instant.now());
        mvManager.register(def);
    }
}
