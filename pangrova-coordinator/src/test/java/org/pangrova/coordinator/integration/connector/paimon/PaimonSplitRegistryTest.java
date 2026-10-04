package org.pangrova.coordinator.integration.connector.paimon;

import org.pangrova.connector.paimon.PaimonSplitRegistry;
import org.pangrova.coordinator.task.TestConnectorQuery;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real SDK split registrations scoped by the actual engine query creation owner. */
class PaimonSplitRegistryTest {
    @TempDir Path warehouse;
    private PaimonSplitRegistry registry;
    private TestConnectorQuery query;
    private Catalog catalog;
    private Table table;
    private Split split;

    @BeforeEach
    void setUp() throws Exception {
        registry = new PaimonSplitRegistry();
        query = new TestConnectorQuery("registry-query");
        registry.beginQuery(query.authority());
        Options options = new Options();
        options.set("warehouse", warehouse.toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("db", false);
        var name = Identifier.create("db", "rows");
        catalog.createTable(name, new Schema(DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.INT().notNull()))
                .getFields(), List.of(), List.of(), Map.of("file.format", "parquet"), ""), false);
        table = catalog.getTable(name);
        var builder = table.newBatchWriteBuilder();
        try (var write = builder.newWrite(); var commit = builder.newCommit()) {
            write.write(GenericRow.of(17));
            commit.commit(write.prepareCommit());
        }
        var actual = table.newReadBuilder().newScan().plan().splits();
        assertThat(actual).isNotEmpty();
        split = actual.get(0);
    }

    @AfterEach
    void tearDown() throws Exception {
        query.seal();
        registry.releaseQuery(query.authority());
        query.retire();
        registry.close();
        catalog.close();
    }

    @Test
    void consume_returnsRegisteredSplit() throws Exception {
        long id = registry.register(query.authority(), table, split);
        try (var lease = registry.claim(query.authority(), id)) {
            assertThat(lease.split()).isSameAs(split);
            assertThat(lease.table()).isSameAs(table);
            assertThat(registry.size()).isZero();
            assertThatThrownBy(() -> registry.claim(query.authority(), id)).hasMessageContaining("already consumed");
        }
    }

    @Test
    void consume_alreadyConsumed_throwsWithClearMessage() throws Exception {
        long id = registry.register(query.authority(), table, split);
        try (var lease = registry.claim(query.authority(), id)) {
            assertThatThrownBy(() -> registry.claim(query.authority(), id))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("already consumed")
                    .hasMessageContaining(String.valueOf(id));
        }
    }

    @Test
    void consume_neverRegistered_throwsWithClearMessage() {
        long bogusId = Long.MAX_VALUE;
        assertThatThrownBy(() -> registry.claim(query.authority(), bogusId))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("No Paimon SDK Split registered")
                .hasMessageContaining(String.valueOf(bogusId));
    }

    @Test
    void consume_afterInvalidate_throwsNotFoundError() {
        long id = registry.register(query.authority(), table, split);
        releaseQuery();
        assertMissingFromAnotherLiveQuery(id);
    }

    @Test
    void consume_nativeOnlySentinel_throwsClearError() {
        assertThatThrownBy(() -> registry.claim(query.authority(), 0))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("native-only split")
                .hasMessageContaining("JVM read path should not be invoked");
    }

    @Test
    void invalidate_removesFromPendingAndConsumed() {
        long id = registry.register(query.authority(), table, split);
        assertThat(registry.size()).isOne();
        releaseQuery();
        assertThat(registry.size()).isZero();
        assertMissingFromAnotherLiveQuery(id);
    }

    @Test
    void invalidate_consumedSplit_removesConsumedRecord() throws Exception {
        long id = registry.register(query.authority(), table, split);
        try (var lease = registry.claim(query.authority(), id)) {
            assertThatThrownBy(() -> registry.claim(query.authority(), id)).hasMessageContaining("already consumed");
        }
        releaseQuery();
        assertMissingFromAnotherLiveQuery(id);
    }

    @Test
    void clear_removesAllState() throws Exception {
        long first = registry.register(query.authority(), table, split);
        long second = registry.register(query.authority(), table, nextActualSplit());
        try (var lease = registry.claim(query.authority(), first)) { assertThat(lease.split()).isSameAs(split); }
        registry.close();
        assertThat(registry.size()).isZero();
        assertThatThrownBy(() -> registry.claim(query.authority(), second)).hasMessageContaining("registry is closed");
        assertThatThrownBy(() -> registry.claim(query.authority(), first)).hasMessageContaining("registry is closed");
    }

    @Test
    void size_tracksOnlyPendingSplits() throws Exception {
        long first = registry.register(query.authority(), table, split);
        registry.register(query.authority(), table, nextActualSplit());
        assertThat(registry.size()).isEqualTo(2);
        try (var lease = registry.claim(query.authority(), first)) { assertThat(registry.size()).isOne(); }
    }

    @Test
    void register_multipleSplits_assignsUniqueIds() throws Exception {
        Split second = nextActualSplit();
        long firstId = registry.register(query.authority(), table, split);
        long secondId = registry.register(query.authority(), table, second);
        assertThat(firstId).isNotEqualTo(secondId);
        try (var firstLease = registry.claim(query.authority(), firstId);
                var secondLease = registry.claim(query.authority(), secondId)) {
            assertThat(firstLease.split()).isSameAs(split);
            assertThat(secondLease.split()).isSameAs(second);
        }
    }

    @Test
    void configuredConstructor_stillWorks() throws Exception {
        try (var configured = new PaimonSplitRegistry(100, Duration.ofMinutes(5))) {
            configured.beginQuery(query.authority());
            long id = configured.register(query.authority(), table, split);
            try (var lease = configured.claim(query.authority(), id)) { assertThat(lease.split()).isSameAs(split); }
            query.seal();
            configured.releaseQuery(query.authority());
        }
    }

    private Split nextActualSplit() { return table.newReadBuilder().newScan().plan().splits().get(0); }

    @Test
    void anotherActualQueryCannotClaimTheRetainedSdkPayload() {
        long id = registry.register(query.authority(), table, split);
        var foreign = new TestConnectorQuery("foreign-query");
        registry.beginQuery(foreign.authority());
        try {
            assertThatThrownBy(() -> registry.claim(foreign.authority(), id))
                    .hasMessageContaining("belongs to another query");
            assertThat(registry.size()).isOne();
        } finally {
            foreign.seal();
            registry.releaseQuery(foreign.authority());
            foreign.retire();
        }
    }

    @Test
    void lateBeginAndRegistrationCannotReviveTheReleasedActualLifetime() {
        registry.register(query.authority(), table, split);
        releaseQuery();
        assertThatThrownBy(() -> registry.beginQuery(query.authority()))
                .hasMessageContaining("admission is closed");
        assertThatThrownBy(() -> registry.register(query.authority(), table, split))
                .hasMessageContaining("admission is closed");
        assertThat(registry.size()).isZero();
    }

    private void releaseQuery() {
        query.seal();
        registry.releaseQuery(query.authority());
    }

    private void assertMissingFromAnotherLiveQuery(long id) {
        var other = new TestConnectorQuery("other-query");
        registry.beginQuery(other.authority());
        try {
            assertThatThrownBy(() -> registry.claim(other.authority(), id))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("No Paimon SDK Split registered");
        } finally {
            other.seal();
            registry.releaseQuery(other.authority());
            other.retire();
        }
    }
}
