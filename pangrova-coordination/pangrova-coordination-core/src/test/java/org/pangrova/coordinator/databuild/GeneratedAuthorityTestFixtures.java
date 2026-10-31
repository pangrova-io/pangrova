package org.pangrova.coordinator.databuild;

import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.ai.control.store.jdbc.embedding.JdbcEmbeddingModelRegistry;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;
import org.pangrova.security.protocol.DeferredAuthorizationScopeCodec;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.store.jdbc.PersistentRoleStore;
import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.connector.spi.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.connector.spi.coordination.WriteCommitEntry;
import org.pangrova.connector.spi.coordination.WriteCommitFingerprint;
import org.pangrova.connector.spi.coordination.WriteCommitJournal;
import org.pangrova.connector.spi.coordination.WriteDeclarationSnapshot;
import org.pangrova.security.api.UserIdentity;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Actual persistent model, INVOKER permissions and redo anchor for this leaf's generated contract tests. */
final class GeneratedAuthorityTestFixtures {
    private GeneratedAuthorityTestFixtures() {}

    static void provisionModel(DataSource dataSource) {
        var roles = new PersistentRoleStore(dataSource);
        roles.initSchema();
        roles.createRole(Role.of("model_owner"));
        roles.createRole(Role.of("model_invoker"));
        roles.grantRoleToUser(UserRoleAssignment.of("definition-owner", "model_owner"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "model_invoker"));
        var registry = new JdbcEmbeddingModelRegistry(dataSource, "model_owner", "model_invoker");
        registry.initSchema();
        registry.create(new EmbeddingModelDescriptor("catalog.embed",
                new EmbeddingModelDescriptor.ProviderRoute("ollama", "http://localhost:11434", ""),
                new EmbeddingModelDescriptor.EmbeddingContract(768, "", ""),
                new EmbeddingModelDescriptor.ServingLimits(32, 100), 0,
                new EmbeddingModelDescriptor.DeploymentFence("model-v7", "0".repeat(64))),
                new UserIdentity("definition-owner", Set.of(), Map.of(), Instant.EPOCH));
    }

    static DeferredAuthorizationScope scope(DataSource dataSource, QualifiedTableName table,
            ConnectorTableMetadata metadata, GeneratedColumnSet definitions) {
        var declaration = WriteDeclarationSnapshot.capture(table, metadata, definitions.toByteArray(), true);
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            var models = ModelAuthorityFixtures.models().observe(connection, "alice", Set.of("catalog.embed"));
            var read = new DeferredAuthorizationScope.TablePermission(table,
                    Set.of("title", "body", "embedding"), Privilege.SELECT,
                    DeferredAuthorizationScope.ReadMode.SECURITY_TRANSFORMED, GeneratedProtectedInputFixtures.observations());
            var write = new DeferredAuthorizationScope.TablePermission(table, Set.of(), Privilege.INSERT,
                    DeferredAuthorizationScope.ReadMode.NONE, DeferredAuthorizationScope.PolicySnapshot.empty());
            var scope = new DeferredAuthorizationScope(List.of(read, write),
                    Optional.of(new DeferredAuthorizationScope.DefinitionSnapshot(declaration, models)));
            connection.commit();
            return scope;
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot observe fixture model authority", failure);
        }
    }

    static String recordCommit(WriteCommitJournal journal, DeferredAuthorizationScope scope) {
        var receipt = DeferredAuthorizationScopeCodec.encodeWriteReceipt(scope);
        var intent = new WriteCommitFingerprint.Intent("generated-write", "tenant-a", "lake", "sales.documents",
                "INSERT", 40L, new byte[]{42}, "alice", receipt, null);
        String fingerprint = WriteCommitFingerprint.compute(intent, List.of(new byte[]{1}));
        journal.recordIntent(new WriteCommitEntry("generated-write", "lake", "sales.documents",
                WriteCommitEntry.State.PENDING, 1_000L, List.of(new byte[]{1}), "INSERT", fingerprint,
                "tenant-a", "alice", receipt, 40L, new byte[]{42}, fingerprint, null, null, 0, null, null));
        journal.markCommitted("generated-write");
        return fingerprint;
    }
}
