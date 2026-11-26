package org.pangrova.coordinator.query.statement.dml;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.metadata.table.SchemaTableName;

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
        ConnectorSession session = new Session();

        assertThatCode(() -> GeneratedColumnWriteGuard.requireNativeMaterializationPlan(
                metadata(Map.of()), session, TABLE, "INSERT", null, null))
                .doesNotThrowAnyException();

        GeneratedColumnSet definitions = GeneratedColumnSet.newBuilder()
                .setSchemaVersion(1)
                .addDefinitions(GeneratedColumnDefinition.newBuilder()
                        .setTargetColumn("embedding")
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                        .setLogicalModelName("embed-v1")
                        .addSourceColumns("text")
                        .setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                        .setOutputType(VectorLogicalType.create(3)))
                .build();
        assertThatThrownBy(() -> GeneratedColumnWriteGuard.requireNativeMaterializationPlan(
                metadata(GeneratedColumnMetadataCodec.encode(definitions)),
                session, TABLE, "INSERT", null,
                new org.pangrova.security.api.identity.UserIdentity(
                        "alice", java.util.Set.of(), Map.of(), java.time.Instant.EPOCH)))
                .hasMessageContaining("control plane is not installed");
        assertThatThrownBy(() -> GeneratedColumnWriteGuard.requireNativeMaterializationPlan(
                metadata(GeneratedColumnMetadataCodec.encode(definitions)),
                session, TABLE, "INSERT", null, null))
                .hasMessageContaining("authenticated user and resolved tenant");
        assertThatThrownBy(() -> GeneratedColumnWriteGuard.requireNativeMaterializationPlan(
                metadata(GeneratedColumnMetadataCodec.encode(definitions)),
                new Session(Map.of()), TABLE, "INSERT", null,
                new org.pangrova.security.api.identity.UserIdentity(
                        "alice", java.util.Set.of(), Map.of(), java.time.Instant.EPOCH)))
                .hasMessageContaining("authenticated user and resolved tenant");
    }

    private static ConnectorTableMetadata metadata(Map<String, String> properties) {
        return new ConnectorTableMetadata(new SchemaTableName("default", "docs"),
                List.of("text", "embedding"), properties);
    }

    private record Session(Map<String, String> properties) implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        private Session() { this(Map.of("tenant_id", "tenant-a")); }
        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of("test_cat"); }
        @Override public Optional<String> getSchema() { return Optional.of("default"); }
        @Override public Map<String, String> getProperties() { return properties; }
    }
}
