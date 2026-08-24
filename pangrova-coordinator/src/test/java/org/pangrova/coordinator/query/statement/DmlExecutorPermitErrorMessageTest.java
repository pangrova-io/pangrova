package org.pangrova.coordinator.query.statement;

import org.pangrova.coordinator.query.statement.dml.DmlExecutor;
import org.pangrova.coordinator.query.statement.dml.DmlExecutorConfig;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.parser.ast.Literal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for BUG-D48 fix: {@link DmlExecutor} must report the configured
 * {@code maxConcurrentDml} in the error message, not the incorrect
 * {@code availablePermits + queueLength} computation.
 */
class DmlExecutorPermitErrorMessageTest {

    @Test
    @DisplayName("timeout error message contains configured max value (BUG-D48)")
    void errorMessageContainsConfiguredMax() throws Exception {
        int configuredMax = 1;
        long timeoutMs = 100;

        CountDownLatch permitHeld = new CountDownLatch(1);
        CountDownLatch canRelease = new CountDownLatch(1);

        CatalogRegistry blockingRegistry = new CatalogRegistry(new StubMetadataStore()) {
            @Override
            public org.pangrova.metadata.catalog.CatalogHandle getCatalog(String name) {
                permitHeld.countDown();
                try {
                    canRelease.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new org.pangrova.metadata.exception.catalog.CatalogNotFoundException(name);
            }
        };

        DmlExecutor executor = new DmlExecutor(blockingRegistry,
                new DmlExecutorConfig(null, null, null, configuredMax, timeoutMs));

        // Thread 1: holds the single permit via executeInsert (blocks in getCatalog)
        AtomicReference<String> capturedError = new AtomicReference<>();
        Thread holder = new Thread(() -> {
            try {
                executor.executeInsert(dummyInsert());
            } catch (Exception ignored) {
                // Expected: catalog not found after release
            }
        });
        holder.setDaemon(true);
        holder.start();

        // Wait for holder to acquire permit and enter getCatalog
        assertThat(permitHeld.await(5, TimeUnit.SECONDS)).isTrue();

        // Thread 2 (main): try to acquire — should timeout with correct message
        try {
            executor.executeInsert(dummyInsert());
        } catch (IllegalStateException e) {
            capturedError.set(e.getMessage());
        } finally {
            canRelease.countDown();
            holder.join(5000);
        }

        assertThat(capturedError.get()).isNotNull();
        // BUG-D48 fix: message must contain the configured max, not a computed value
        assertThat(capturedError.get()).contains("max=" + configuredMax);
        assertThat(capturedError.get()).contains("timed out after " + timeoutMs + "ms");
    }

    @Test
    @DisplayName("default max concurrent DML constant is 16")
    void defaultMaxConcurrentDmlIs16() {
        assertThat(DmlExecutorConfig.DEFAULT_MAX_CONCURRENT_DML).isEqualTo(16);
    }

    private static InsertStatement dummyInsert() {
        return new InsertStatement(
                new Identifier(List.of("catalog", "db", "table")),
                Optional.of(List.of("col1")),
                List.of(List.of(Literal.ofInteger(1))));
    }

    /** Minimal MetadataStore stub — no methods are actually called. */
    static final class StubMetadataStore implements MetadataStore {
        @Override public void saveTable(TableMetadata m) {}
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) {}
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) {}
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) {}
        @Override public void recordSyncHistory(SyncResult r) {}
        @Override public void saveSchemas(String c, List<String> s) {}
    }
}
