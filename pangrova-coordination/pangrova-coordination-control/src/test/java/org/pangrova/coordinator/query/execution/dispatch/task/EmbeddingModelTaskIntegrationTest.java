/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.query.execution.dispatch.task;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.ai.control.embedding.ConcurrentMapEmbeddingModelRegistry;
import org.pangrova.ai.control.model.EmbeddingModelExecutionAdmission;
import org.pangrova.ai.control.exception.EmbeddingRegistryAccessDeniedException;
import org.pangrova.ai.control.exception.ModelDropUnavailableException;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.parser.parse.AntlrSqlParser;
import org.pangrova.parser.api.ast.statement.CreateEmbeddingModelStatement;
import org.pangrova.parser.api.ast.statement.AlterEmbeddingModelStatement;
import org.pangrova.parser.api.ast.statement.DropEmbeddingModelStatement;
import org.pangrova.parser.api.ast.statement.ShowEmbeddingModelsStatement;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.CapabilityLimits;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.protocol.v2.ModelCapabilityContract;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.ai.api.embedding.EmbeddingModelBindingIdentity;
import org.pangrova.security.api.identity.UserIdentity;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingModelTaskIntegrationTest {

    private static final String ADMIN_GROUP = "embedding_admin";
    private static final String USAGE_GROUP = "embedding_user";
    private static final UserIdentity ADMIN = new UserIdentity(
            "admin", Set.of(ADMIN_GROUP), Map.of(), Instant.now());

    @Test
    void parserToCreateTaskPersistsExactDeploymentFence() {
        ConcurrentMapEmbeddingModelRegistry registry = registry();
        ModelAvailability availability = availability();
        CreateEmbeddingModelTask task = new CreateEmbeddingModelTask(
                registry, (model, capability) -> availability);
        CreateEmbeddingModelStatement statement = (CreateEmbeddingModelStatement) new AntlrSqlParser()
                .parse("CREATE EMBEDDING MODEL catalog_embed ("
                        + "provider = 'openai-compatible', "
                        + "endpoint = 'https://example.test/embeddings', "
                        + "dimension = 768, "
                        + "api_key_secret = 'vault://embedding-key', "
                        + "max_batch_size = 32, "
                        + "max_requests_per_second = 75)");

        task.execute(statement, context(ADMIN));

        EmbeddingModelDescriptor stored = registry.find("catalog_embed", ADMIN)
                .orElseThrow();
        assertThat(stored.maxRequestsPerSecond()).isEqualTo(75);
        assertThat(stored.deploymentVersion()).isEqualTo("model-v7");
        assertThat(stored.deploymentSignatureSha256()).isEqualTo("04" + "00".repeat(31));
    }

    @Test
    void explicitlyUnauthenticatedLoopbackModelBindsWithoutSecret() {
        ConcurrentMapEmbeddingModelRegistry registry = registry();
        EmbeddingModelDescriptor requested = new EmbeddingModelDescriptor(
                "catalog_embed",
                new EmbeddingModelDescriptor.ProviderRoute(
                        "openai-compatible", "http://127.0.0.1:11434/v1/embeddings", ""),
                new EmbeddingModelDescriptor.EmbeddingContract(768, "", ""),
                new EmbeddingModelDescriptor.ServingLimits(32, 75),
                0, EmbeddingModelDescriptor.DeploymentFence.unbound());
        CreateEmbeddingModelTask task = new CreateEmbeddingModelTask(
                registry, (model, capability) -> availability(requested, "ollama-v1", 9));
        CreateEmbeddingModelStatement statement = (CreateEmbeddingModelStatement) new AntlrSqlParser()
                .parse("CREATE EMBEDDING MODEL catalog_embed ("
                        + "provider = 'openai-compatible', "
                        + "endpoint = 'http://127.0.0.1:11434/v1/embeddings', "
                        + "dimension = 768, authentication = 'none', "
                        + "max_batch_size = 32, max_requests_per_second = 75)");

        task.execute(statement, context(ADMIN));

        EmbeddingModelDescriptor stored = registry.find("catalog_embed", ADMIN).orElseThrow();
        assertThat(stored.route().unauthenticated()).isTrue();
        assertThat(stored.apiKeySecret()).isEmpty();
        assertThat(stored.deploymentVersion()).isEqualTo("ollama-v1");
    }

    @Test
    void unauthenticatedModelCannotDeclareSecretOrUseRemotePlaintextEndpoint() {
        CreateEmbeddingModelTask task = new CreateEmbeddingModelTask(
                registry(), (model, capability) -> availability());
        String prefix = "CREATE EMBEDDING MODEL catalog_embed ("
                + "provider = 'openai-compatible', dimension = 768, authentication = 'none', ";
        assertThatThrownBy(() -> task.execute((CreateEmbeddingModelStatement) new AntlrSqlParser()
                .parse(prefix + "endpoint = 'http://127.0.0.1:11434/v1/embeddings', "
                        + "api_key_secret = 'vault://key')"), context(ADMIN)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not declare api_key_secret");
        assertThatThrownBy(() -> task.execute((CreateEmbeddingModelStatement) new AntlrSqlParser()
                .parse(prefix + "endpoint = 'http://public.example/v1/embeddings')"),
                context(ADMIN)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("anonymous loopback");
    }

    @Test
    void rejectsLegacyRateAliasInsteadOfSilentlyDefaultingIt() {
        CreateEmbeddingModelTask task = new CreateEmbeddingModelTask(
                registry(), (model, capability) -> availability());
        CreateEmbeddingModelStatement statement = (CreateEmbeddingModelStatement) new AntlrSqlParser()
                .parse("CREATE EMBEDDING MODEL catalog_embed ("
                        + "provider = 'openai-compatible', "
                        + "endpoint = 'https://example.test/embeddings', "
                        + "dimension = 768, "
                        + "api_key_secret = 'vault://embedding-key', max_rps = 75)");

        assertThatThrownBy(() -> task.execute(statement, context(ADMIN)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported embedding model property: max_rps");
    }

    @Test
    void missingIdentityFailsClosedBeforeWorkerResolutionOrRegistryAccess() {
        AtomicBoolean resolverCalled = new AtomicBoolean();
        ConcurrentMapEmbeddingModelRegistry registry = registry();
        CreateEmbeddingModelTask create = new CreateEmbeddingModelTask(registry, (model, capability) -> {
            resolverCalled.set(true);
            return availability();
        });
        CreateEmbeddingModelStatement statement = new CreateEmbeddingModelStatement(
                "catalog_embed", Map.of());

        assertAccessDenied(() -> create.execute(statement, context(null)));
        assertThat(resolverCalled).isFalse();
        assertThat(registry.find("catalog_embed", ADMIN)).isEmpty();
        assertAccessDenied(() -> new DropEmbeddingModelTask(registry).execute(
                new DropEmbeddingModelStatement("catalog_embed", false), context(null)));
        assertAccessDenied(() -> new ShowEmbeddingModelsTask(registry).execute(
                new ShowEmbeddingModelsStatement(), context(null)));
    }

    @Test
    void parserToDropTaskRetainsModelForBothCascadeModes() {
        ConcurrentMapEmbeddingModelRegistry registry = registry();
        registry.create(EmbeddingModelExecutionAdmission.bind(
                requestedDescriptor(), availability()), ADMIN);
        DropEmbeddingModelTask task = new DropEmbeddingModelTask(registry);
        for (String sql : Set.of(
                "DROP EMBEDDING MODEL catalog_embed",
                "DROP EMBEDDING MODEL catalog_embed CASCADE")) {
            DropEmbeddingModelStatement statement = (DropEmbeddingModelStatement)
                    new AntlrSqlParser().parse(sql);
            assertThatThrownBy(() -> task.execute(statement, context(ADMIN)))
                    .isInstanceOf(ModelDropUnavailableException.class)
                    .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                            .isEqualTo(PangrovaErrorCode.AI_EMBEDDING_MODEL_DROP_UNAVAILABLE));
            assertThat(registry.find("catalog_embed", ADMIN)).isPresent();
        }
    }

    @Test
    void missingCreatePrivilegeFailsBeforeWorkerResolution() {
        AtomicBoolean resolverCalled = new AtomicBoolean();
        ConcurrentMapEmbeddingModelRegistry registry = registry();
        CreateEmbeddingModelTask create = new CreateEmbeddingModelTask(registry, (model, capability) -> {
            resolverCalled.set(true);
            return availability();
        });
        UserIdentity unprivileged = new UserIdentity(
                "reader", Set.of(USAGE_GROUP), Map.of(), Instant.now());

        assertThatThrownBy(() -> create.execute(
                new CreateEmbeddingModelStatement("catalog_embed", Map.of()),
                context(unprivileged)))
                .isInstanceOf(EmbeddingRegistryAccessDeniedException.class);
        assertThat(resolverCalled).isFalse();
    }

    @Test
    void parserToAlterTaskAtomicallyPersistsNextBoundRevision() {
        ConcurrentMapEmbeddingModelRegistry registry = registry();
        EmbeddingModelDescriptor initial = EmbeddingModelExecutionAdmission.bind(
                requestedDescriptor(), availability());
        registry.create(initial, ADMIN);
        EmbeddingModelDescriptor requested = new EmbeddingModelDescriptor(
                initial.name(),
                new EmbeddingModelDescriptor.ProviderRoute(
                        initial.provider(), "https://new.example.test/embeddings",
                        initial.apiKeySecret()),
                initial.contract(), initial.limits(), 1,
                EmbeddingModelDescriptor.DeploymentFence.unbound());
        ModelAvailability nextAvailability = availability(requested, "model-v8", 8);
        AlterEmbeddingModelTask task = new AlterEmbeddingModelTask(
                registry, (model, capability) -> nextAvailability);
        AlterEmbeddingModelStatement statement = (AlterEmbeddingModelStatement) new AntlrSqlParser()
                .parse("ALTER EMBEDDING MODEL catalog_embed SET ("
                        + "expected_version = 0, "
                        + "endpoint = 'https://new.example.test/embeddings')");

        task.execute(statement, context(ADMIN));

        EmbeddingModelDescriptor stored = registry.find("catalog_embed", ADMIN).orElseThrow();
        assertThat(stored.version()).isEqualTo(1);
        assertThat(stored.endpoint()).isEqualTo("https://new.example.test/embeddings");
        assertThat(stored.deploymentVersion()).isEqualTo("model-v8");
        assertThat(stored.deploymentSignatureSha256()).isEqualTo("08" + "00".repeat(31));
    }

    @Test
    void alterRequiresAdminBeforeResolvingWorkerMetadata() {
        AtomicBoolean resolverCalled = new AtomicBoolean();
        ConcurrentMapEmbeddingModelRegistry registry = registry();
        registry.create(EmbeddingModelExecutionAdmission.bind(
                requestedDescriptor(), availability()), ADMIN);
        AlterEmbeddingModelTask task = new AlterEmbeddingModelTask(registry, (model, capability) -> {
            resolverCalled.set(true);
            return availability();
        });
        AlterEmbeddingModelStatement statement = new AlterEmbeddingModelStatement(
                "catalog_embed", Map.of("expected_version", "0", "max_batch_size", "16"));
        UserIdentity unprivileged = new UserIdentity(
                "reader", Set.of(USAGE_GROUP), Map.of(), Instant.now());

        assertThatThrownBy(() -> task.execute(statement, context(unprivileged)))
                .isInstanceOf(EmbeddingRegistryAccessDeniedException.class);
        assertThat(resolverCalled).isFalse();
    }

    @Test
    void alterRequiresExplicitCurrentVersionBeforeResolvingWorkerMetadata() {
        AtomicBoolean resolverCalled = new AtomicBoolean();
        ConcurrentMapEmbeddingModelRegistry registry = registry();
        registry.create(EmbeddingModelExecutionAdmission.bind(
                requestedDescriptor(), availability()), ADMIN);
        AlterEmbeddingModelTask task = new AlterEmbeddingModelTask(registry, (model, capability) -> {
            resolverCalled.set(true);
            return availability();
        });

        assertThatThrownBy(() -> task.execute(
                new AlterEmbeddingModelStatement(
                        "catalog_embed", Map.of("endpoint", "https://new.test/embedding")),
                context(ADMIN)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expected_version");
        assertThat(resolverCalled).isFalse();
    }

    @Test
    void staleAlterVersionFailsBeforeResolvingWorkerMetadata() {
        AtomicBoolean resolverCalled = new AtomicBoolean();
        ConcurrentMapEmbeddingModelRegistry registry = registry();
        registry.create(EmbeddingModelExecutionAdmission.bind(
                requestedDescriptor(), availability()), ADMIN);
        AlterEmbeddingModelTask task = new AlterEmbeddingModelTask(registry, (model, capability) -> {
            resolverCalled.set(true);
            return availability();
        });

        assertThatThrownBy(() -> task.execute(
                new AlterEmbeddingModelStatement(
                        "catalog_embed",
                        Map.of("expected_version", "7", "max_batch_size", "16")),
                context(ADMIN)))
                .hasMessageContaining("expected version 7");
        assertThat(resolverCalled).isFalse();
    }

    @Test
    void deploymentMismatchLeavesCurrentRevisionUnchanged() {
        ConcurrentMapEmbeddingModelRegistry registry = registry();
        EmbeddingModelDescriptor initial = EmbeddingModelExecutionAdmission.bind(
                requestedDescriptor(), availability());
        registry.create(initial, ADMIN);
        AlterEmbeddingModelTask task = new AlterEmbeddingModelTask(
                registry, (model, capability) -> availability());

        assertThatThrownBy(() -> task.execute(
                new AlterEmbeddingModelStatement(
                        "catalog_embed", Map.of(
                                "expected_version", "0",
                                "endpoint", "https://new.example.test/embeddings")),
                context(ADMIN)))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("does not match catalog contract");
        assertThat(registry.find("catalog_embed", ADMIN)).contains(initial);
    }

    @Test
    void parserRejectsDuplicateExpectedVersion() {
        assertThatThrownBy(() -> new AntlrSqlParser().parse(
                "ALTER EMBEDDING MODEL catalog_embed SET ("
                        + "expected_version = 0, expected_version = 1, max_batch_size = 16)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate embedding model property: expected_version");
    }

    private static void assertAccessDenied(Runnable operation) {
        assertThatThrownBy(operation::run)
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    private static ConcurrentMapEmbeddingModelRegistry registry() {
        return new ConcurrentMapEmbeddingModelRegistry(ADMIN_GROUP, USAGE_GROUP);
    }

    private static StatementExecutionContext context(UserIdentity identity) {
        return new StatementExecutionContext(
                null,
                "query-1",
                new QuerySession("query-1", Map.of(), identity, "tenant-1", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture()));
    }

    private static ModelAvailability availability() {
        return availability(requestedDescriptor(), "model-v7", 4);
    }

    private static ModelAvailability availability(
            EmbeddingModelDescriptor descriptor, String version, int signatureFirstByte) {
        byte[] signature = new byte[32];
        signature[0] = (byte) signatureFirstByte;
        return ModelAvailability.newBuilder()
                .setModel(VersionedRef.newBuilder()
                        .setId("catalog_embed")
                        .setVersion(version)
                        .setSignatureSha256(ByteString.copyFrom(signature)))
                .addCapabilityContracts(ModelCapabilityContract.newBuilder()
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                        .setOutputType(VectorLogicalType.create(768))
                        .setBindingContractSha256(ByteString.copyFrom(
                                EmbeddingModelBindingIdentity.sha256(descriptor))))
                .setLimits(CapabilityLimits.newBuilder()
                        .setMaxBatchItems(32)
                        .setMaxInputTokens(8_192)
                        .setMaxOutputTokens(1)
                        .setMaxConcurrency(4)
                        .setMaxProviderItemsPerTask(100)
                                .setMaxProviderAttemptsPerTask(100)
                                .setMaxRequestBytes(1_048_576)
                                .setMaxResponseBytes(2_097_152))
                .build();
    }

    private static EmbeddingModelDescriptor requestedDescriptor() {
        return new EmbeddingModelDescriptor(
                "catalog_embed",
                new EmbeddingModelDescriptor.ProviderRoute(
                        "openai-compatible", "https://example.test/embeddings",
                        "vault://embedding-key"),
                new EmbeddingModelDescriptor.EmbeddingContract(768, "", ""),
                new EmbeddingModelDescriptor.ServingLimits(32, 75),
                0,
                EmbeddingModelDescriptor.DeploymentFence.unbound());
    }
}
