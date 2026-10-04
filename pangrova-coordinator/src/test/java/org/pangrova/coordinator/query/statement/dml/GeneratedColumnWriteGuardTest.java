package org.pangrova.coordinator.query.statement.dml;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeneratedColumnWriteGuardTest {

    private static final QualifiedTableName TABLE =
            new QualifiedTableName("test_cat", "default", "docs");

    @Test
    void permitsOrdinaryTableAndRejectsUnmaterializedGeneratedWrite() {
        ConnectorTableHandle handle = new Handle();
        ConnectorSession session = new Session();

        assertThatCode(() -> GeneratedColumnWriteGuard.requireNativeMaterializationPlan(
                metadata(Map.of()), session, handle, TABLE, "INSERT", null, null))
                .doesNotThrowAnyException();

        GeneratedColumnSet definitions = GeneratedColumnSet.newBuilder()
                .setSchemaVersion(1)
                .addDefinitions(GeneratedColumnDefinition.newBuilder()
                        .setTargetColumn("embedding")
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                        .setLogicalModelName("embed-v1")
                        .addSourceColumns("text")
                        .setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                        .setOutputType(LogicalType.newBuilder()
                                .setCanonicalSql("VECTOR(3, COSINE)")))
                .build();
        assertThatThrownBy(() -> GeneratedColumnWriteGuard.requireNativeMaterializationPlan(
                metadata(GeneratedColumnMetadataCodec.encode(definitions)),
                session, handle, TABLE, "INSERT", null,
                new org.pangrova.spi.security.UserIdentity(
                        "alice", java.util.Set.of(), Map.of(), java.time.Instant.EPOCH)))
                .hasMessageContaining("control plane is not installed");
        assertThatThrownBy(() -> GeneratedColumnWriteGuard.requireNativeMaterializationPlan(
                metadata(GeneratedColumnMetadataCodec.encode(definitions)),
                session, handle, TABLE, "INSERT", null, null))
                .hasMessageContaining("authenticated user and resolved tenant");
        assertThatThrownBy(() -> GeneratedColumnWriteGuard.requireNativeMaterializationPlan(
                metadata(GeneratedColumnMetadataCodec.encode(definitions)),
                new Session(Map.of()), handle, TABLE, "INSERT", null,
                new org.pangrova.spi.security.UserIdentity(
                        "alice", java.util.Set.of(), Map.of(), java.time.Instant.EPOCH)))
                .hasMessageContaining("authenticated user and resolved tenant");
    }

    private static ConnectorMetadata metadata(Map<String, String> properties) {
        return new ConnectorMetadata() {
            @Override
            public ConnectorTableHandle getTableHandle(
                    ConnectorSession session, SchemaTableName tableName) {
                return new Handle();
            }

            @Override
            public ConnectorTableMetadata getTableMetadata(
                    ConnectorSession session, ConnectorTableHandle table) {
                return new ConnectorTableMetadata(
                        new SchemaTableName("default", "docs"),
                        List.of("text", "embedding"), properties);
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(
                    ConnectorSession session, ConnectorTableHandle table) {
                return Map.of();
            }

            @Override
            public TableStatistics getTableStatistics(
                    ConnectorSession session, ConnectorTableHandle table) {
                return TableStatistics.UNKNOWN;
            }

            @Override
            public List<String> listSchemaNames(ConnectorSession session) {
                return List.of("default");
            }

            @Override
            public List<SchemaTableName> listTables(
                    ConnectorSession session, Optional<String> schemaName) {
                return List.of(new SchemaTableName("default", "docs"));
            }
        };
    }

    private record Handle() implements ConnectorTableHandle {}

    private record Session(Map<String, String> properties) implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        private Session() { this(Map.of("tenant_id", "tenant-a")); }
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test_cat"); }
        @Override public Optional<String> getSchema() { return Optional.of("default"); }
        @Override public Map<String, String> getProperties() { return properties; }
    }
}
