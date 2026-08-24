package org.pangrova.planner.mv;

import org.pangrova.parser.ast.RefreshStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests merge-on-read resolution in {@link DynamicTableRewriter}.
 *
 * <p>When a DT has pending deltas (lastRefreshSnapshot &lt; currentSnapshot),
 * the rewriter returns {@link DynamicTableResolution.MergeOnRead} so the planner
 * can generate a UNION ALL plan combining materialized data with incremental delta.</p>
 */
class DynamicTableMergeOnReadTest {

    private MaterializedViewRegistry mvManager;

    @BeforeEach
    void setUp() {
        mvManager = new MaterializedViewRegistry();
    }

    @Test
    void pendingDeltas_returnsMergeOnRead() {
        registerDt("sales_summary");
        SnapshotInfoProvider provider = new StubSnapshotInfoProvider(
                "sales_summary", 100L, "source_table", 200L);
        DynamicTableRewriter rewriter = new DynamicTableRewriter(mvManager, provider);

        DynamicTableResolution resolution = rewriter.resolve("sales_summary");

        assertThat(resolution).isInstanceOf(DynamicTableResolution.MergeOnRead.class);
        DynamicTableResolution.MergeOnRead mor = (DynamicTableResolution.MergeOnRead) resolution;
        assertThat(mor.materializedTable()).isEqualTo("__dt_sales_summary");
        assertThat(mor.lastRefreshSnapshot()).isEqualTo(100L);
        assertThat(mor.sourceTable()).isEqualTo("source_table");
        assertThat(mor.currentSnapshot()).isEqualTo(200L);
    }

    @Test
    void fullyRefreshed_returnsDirectTarget() {
        registerDt("sales_summary");
        SnapshotInfoProvider provider = new StubSnapshotInfoProvider(
                "sales_summary", 200L, "source_table", 200L);
        DynamicTableRewriter rewriter = new DynamicTableRewriter(mvManager, provider);

        DynamicTableResolution resolution = rewriter.resolve("sales_summary");

        assertThat(resolution).isInstanceOf(DynamicTableResolution.DirectTarget.class);
        DynamicTableResolution.DirectTarget dt = (DynamicTableResolution.DirectTarget) resolution;
        assertThat(dt.tableName()).isEqualTo("__dt_sales_summary");
    }

    @Test
    void noSnapshotProvider_returnsDirectTarget() {
        registerDt("sales_summary");
        DynamicTableRewriter rewriter = new DynamicTableRewriter(mvManager);

        DynamicTableResolution resolution = rewriter.resolve("sales_summary");

        assertThat(resolution).isInstanceOf(DynamicTableResolution.DirectTarget.class);
    }

    @Test
    void resolveTargetTable_mergeOnRead_returnsMaterializedTable() {
        registerDt("sales_summary");
        SnapshotInfoProvider provider = new StubSnapshotInfoProvider(
                "sales_summary", 100L, "source_table", 200L);
        DynamicTableRewriter rewriter = new DynamicTableRewriter(mvManager, provider);

        Optional<String> target = rewriter.resolveTargetTable("sales_summary");

        assertThat(target).isPresent().hasValue("__dt_sales_summary");
    }

    @Test
    void snapshotProviderThrows_fallsBackToDirectTarget() {
        registerDt("sales_summary");
        SnapshotInfoProvider failingProvider = new SnapshotInfoProvider() {
            @Override
            public OptionalLong lastRefreshSnapshotId(String dtName) {
                throw new RuntimeException("metadata unavailable");
            }

            @Override
            public String sourceTable(String dtName) {
                return null;
            }

            @Override
            public OptionalLong currentSnapshotId(String sourceTable) {
                return OptionalLong.empty();
            }
        };
        DynamicTableRewriter rewriter = new DynamicTableRewriter(mvManager, failingProvider);

        DynamicTableResolution resolution = rewriter.resolve("sales_summary");

        assertThat(resolution).isInstanceOf(DynamicTableResolution.DirectTarget.class);
    }

    @Test
    void targetOverride_mergeOnRead_usesOverrideTable() {
        registerDt("cdc_table");
        SnapshotInfoProvider provider = new StubSnapshotInfoProvider(
                "cdc_table", 50L, "raw_events", 100L);
        DynamicTableRewriter rewriter = new DynamicTableRewriter(mvManager, provider);
        rewriter.registerTargetOverride("cdc_table", "custom_target");

        DynamicTableResolution resolution = rewriter.resolve("cdc_table");

        assertThat(resolution).isInstanceOf(DynamicTableResolution.MergeOnRead.class);
        DynamicTableResolution.MergeOnRead mor = (DynamicTableResolution.MergeOnRead) resolution;
        assertThat(mor.materializedTable()).isEqualTo("custom_target");
    }

    private void registerDt(String name) {
        MaterializedViewDefinition def = new MaterializedViewDefinition(
                name,
                "SELECT * FROM source_table",
                RefreshStrategy.INCREMENTAL,
                true,
                Optional.of(Instant.now()),
                Instant.now());
        mvManager.register(def);
    }

    /**
     * Stub SnapshotInfoProvider for testing merge-on-read resolution.
     */
    private record StubSnapshotInfoProvider(
            String dtName,
            long lastRefresh,
            String source,
            long currentSnap
    ) implements SnapshotInfoProvider {

        @Override
        public OptionalLong lastRefreshSnapshotId(String dtName) {
            if (this.dtName.equals(dtName)) {
                return OptionalLong.of(lastRefresh);
            }
            return OptionalLong.empty();
        }

        @Override
        public String sourceTable(String dtName) {
            if (this.dtName.equals(dtName)) {
                return source;
            }
            return null;
        }

        @Override
        public OptionalLong currentSnapshotId(String sourceTable) {
            if (this.source.equals(sourceTable)) {
                return OptionalLong.of(currentSnap);
            }
            return OptionalLong.empty();
        }
    }
}
