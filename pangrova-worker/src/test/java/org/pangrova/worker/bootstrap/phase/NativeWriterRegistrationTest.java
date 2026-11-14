package org.pangrova.worker.bootstrap.phase;

import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSink;
import org.pangrova.connector.api.data.ConnectorPageSinkProvider;
import org.pangrova.batch.api.page.Page;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
import org.pangrova.worker.catalog.CatalogResourceResolver;
import org.pangrova.worker.write.NativeWriteExecutor;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that the native write path registration in {@link WorkerTaskBootstrap}
 * correctly wires the {@link NativeWriteExecutor} when a catalog resolver is provided,
 * and rejects a native-capable Worker when no resolver is injected.
 */
class NativeWriterRegistrationTest {
    private WorkerComponentRefs nativeRefs;

    @AfterEach
    void releaseNativeRuntime() {
        if (nativeRefs != null) {
            try {
                if (nativeRefs.nativeWriteExecutor() != null) nativeRefs.nativeWriteExecutor().close();
            } finally {
                org.pangrova.worker.jni.NativeRuntime.shutdownNativeRuntime();
            }
        }
    }

    private WorkerComponentRefs initializedRefs() {
        WorkerComponentRefs refs = new WorkerComponentRefs();
        NativeRuntimeBootstrap.initialize(org.pangrova.testing.nativefixture.TestShuffleServiceConfig.config(
                org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig.withDefaults("writer-registration")), refs);
        nativeRefs = refs;
        return refs;
    }

    @Test
    void registerNativeWriter_skipsWhenNativeUnavailable() {
        WorkerComponentRefs refs = new WorkerComponentRefs();
        refs.setNativeAvailable(false);
        refs.setCatalogResourceResolver(buildStubResolver());

        WorkerTaskBootstrap.registerNativeWriter(refs);

        assertThat(refs.nativeWriteExecutor()).isNull();
    }

    @Test
    void registerNativeWriterRejectsMissingCatalogRuntime() {
        WorkerComponentRefs refs = new WorkerComponentRefs();
        refs.setNativeAvailable(true);

        assertThatThrownBy(() -> WorkerTaskBootstrap.registerNativeWriter(refs))
                .isInstanceOf(org.pangrova.common.exception.BootstrapFailedException.class)
                .hasMessageContaining("NativeWriter");
    }

    @Test
    @Tag("native-required")
    void registerNativeWriter_registersWhenNativeAvailableAndResolverPresent() {
        WorkerComponentRefs refs = initializedRefs();
        refs.setCatalogResourceResolver(buildStubResolver());

        WorkerTaskBootstrap.registerNativeWriter(refs);

        assertThat(refs.nativeWriteExecutor()).isNotNull();
        assertThat(refs.nativeWriteExecutor().activeSessionCount()).isZero();
    }

    @Test
    @Tag("native-required")
    void nativeWriteExecutor_canBeginAndAbortWriteSession() {
        WorkerComponentRefs refs = initializedRefs();
        refs.setCatalogResourceResolver(buildStubResolver());

        WorkerTaskBootstrap.registerNativeWriter(refs);

        NativeWriteExecutor writer = refs.nativeWriteExecutor();
        assertThat(writer).isNotNull();

        var descriptor = new org.pangrova.worker.write.WriteDescriptor(
                org.pangrova.worker.write.WriteDescriptor.DmlOperation.INSERT,
                List.of(new org.pangrova.worker.write.WriteDescriptor.ColumnSchema("id", "INTEGER"),
                        new org.pangrova.worker.write.WriteDescriptor.ColumnSchema("name", "VARCHAR")),
                java.util.Map.of("taskId", "native-registration-test"));

        long handle = writer.beginWrite(descriptor);
        assertThat(handle).isPositive();
        assertThat(writer.activeSessionCount()).isEqualTo(1);

        writer.abortWrite(handle);
        assertThat(writer.activeSessionCount()).isZero();
    }

    private static CatalogResourceResolver buildStubResolver() {
        return new org.pangrova.worker.write.TestCatalogResourceResolver((taskId) -> {
            ConnectorMetadata metadata = new StubMetadata();
            ConnectorPageSinkProvider provider = (txn, session, outputHandle) -> new StubPageSink();
            ConnectorSession session = new StubSession();
            return new CatalogResourceResolver.CatalogResources(metadata, provider, session,
                    org.pangrova.worker.write.TestOutputHandles.outputHandle(),
                    java.util.List.of("id", "name"));
        });
    }

    private static final class StubPageSink implements ConnectorPageSink {
        @Override
        public void appendPage(Page page) {
            // intentional no-op: tests focus on registration lifecycle
        }

        @Override
        public byte[] finishWithMetadata() { return new byte[0]; }

        @Override
        public void abort() {}

        @Override
        public void close() {}
    }

    private static final class StubMetadata implements ConnectorMetadata {
        private static final ConnectorTableHandle TABLE_HANDLE = new ConnectorTableHandle() {};
        private static final ConnectorOutputTableHandle OUTPUT_HANDLE = new ConnectorOutputTableHandle() {};

        @Override
        public List<String> listSchemaNames(ConnectorSession session) { return List.of(); }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, java.util.Optional<String> schemaName) {
            return List.of();
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            return TABLE_HANDLE;
        }

        @Override
        public org.pangrova.connector.api.metadata.table.ConnectorTableMetadata getTableMetadata(
                ConnectorSession session, ConnectorTableHandle table) {
            return null;
        }

        @Override
        public java.util.Map<String, org.pangrova.connector.api.metadata.table.ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle table) {
            return java.util.Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public ConnectorOutputTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle) {
            return OUTPUT_HANDLE;
        }

        @Override
        public ConnectorOutputTableHandle beginOverwrite(ConnectorSession session, ConnectorTableHandle tableHandle) {
            return OUTPUT_HANDLE;
        }

        @Override
        public ConnectorOutputTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle) {
            return OUTPUT_HANDLE;
        }
    }

    private static final class StubSession implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test-user"; }
        @Override public java.util.Optional<String> getCatalog() { return java.util.Optional.of("test-catalog"); }
        @Override public java.util.Optional<String> getSchema() { return java.util.Optional.of("test-schema"); }
        @Override public java.util.Map<String, String> getProperties() { return java.util.Map.of(); }
    }
}
