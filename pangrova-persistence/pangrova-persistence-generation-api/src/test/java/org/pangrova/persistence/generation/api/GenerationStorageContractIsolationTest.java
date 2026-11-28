package org.pangrova.persistence.generation.api;

import org.junit.jupiter.api.Test;
import org.pangrova.common.crypto.Sha256Digests;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.shuffle.api.descriptor.ShuffleProviderDescriptor;
import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.execution.api.ownership.ExecutionAnchor;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies isolated value and storage contracts and the real API classpath boundary. */
class GenerationStorageContractIsolationTest {
    @Test
    void graphEvidenceAndDispatchValuesDoNotLinkSerializationOrStoreImplementations() throws Exception {
        URL[] locations = {
                GenerationDispatchSpec.class.getProtectionDomain().getCodeSource().getLocation(),
                Sha256Digests.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.persistence.generation.api.job.GenerationJobStore.class
                        .getProtectionDomain().getCodeSource().getLocation(),
                ShuffleProviderDescriptor.class.getProtectionDomain().getCodeSource().getLocation(),
                ExecutionAnchor.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.connector.api.metadata.table.QualifiedTableName.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.types.Type.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.batch.api.page.Page.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.security.api.identity.UserIdentity.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.persistence.query.api.commit.WriteCommitEntry.class
                        .getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader())) {
            for (String missing : List.of("com.fasterxml.jackson.databind.ObjectMapper",
                    "org.pangrova.shuffle.pangrova.client.PangrovaShuffleClient",
                    "org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority",
                    "org.pangrova.generation.submission.GenerationStageGraphFactory",
                    "org.pangrova.generation.codec.execution.GenerationStageGraphCodec",
                    "org.pangrova.generation.codec.execution.GenerationExchangeEvidenceCodec",
                    "org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore",
                    "org.pangrova.persistence.generation.postgresql.job.PostgresGenerationStageExecutionStore",
                    "org.pangrova.persistence.generation.postgresql.exchange.PostgresGenerationStageExchangeStore",
                    "org.pangrova.persistence.generation.postgresql.execution.PostgresCurrentGenerationAuthority",
                    "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                    "org.pangrova.persistence.query.postgresql.coordination.JdbcExecutionAnchorStore",
                    "org.postgresql.Driver",
                    "org.pangrova.metadata.codec.table.TableMetadataSerializer")) {
                assertThatThrownBy(() -> loader.loadClass(missing)).isInstanceOf(ClassNotFoundException.class);
            }
            for (var entry : java.util.Map.of(locations[0], "org/pangrova/generation/api",
                    locations[2], "org/pangrova/persistence/generation/api").entrySet()) {
                for (String name : contractNames(entry.getKey(), entry.getValue())) {
                    Class<?> type = loader.loadClass(name);
                    assertThat(type.getClassLoader()).isSameAs(loader);
                    type.getMethods();
                    type.getConstructors();
                    type.getRecordComponents();
                }
            }
            requireStorageContractsWithoutJdbc(loader);
            Class<?> graphType = loader.loadClass("org.pangrova.generation.api.graph.GenerationStageGraph");
            Class<?> templateType = loader.loadClass(
                    "org.pangrova.generation.api.graph.template.GenerationStageTemplate");
            Object template = templateType.getMethod("of", byte[].class, List.class, List.class)
                    .invoke(null, new byte[]{2}, List.of(), List.of());
            Class<?> kindType = loader.loadClass("org.pangrova.generation.api.enums.GenerationPageKind");
            Class<?> stageType = loader.loadClass(GenerationStageGraph.StagePlan.class.getName());
            Object stage = stageType.getMethod("of", int.class, int.class, kindType, byte[].class,
                            templateType, List.class, List.class, boolean.class)
                    .invoke(null, 0, 0, kindType.getField("SOURCE_MAP").get(null), new byte[]{1},
                            template, List.of(), List.of(), true);
            Class<?> purposeType = loader.loadClass(GenerationGraphPurpose.class.getName());
            Object graph = graphType.getConstructor(int.class, purposeType, List.class, List.class, List.class)
                    .newInstance(2, purposeType.getField("ARTIFACT_GENERATION").get(null),
                            List.of(stage), List.of(), List.of());
            Class<?> scopeType = loader.loadClass(DeferredAuthorizationScope.class.getName());
            Class<?> tableType = loader.loadClass(QualifiedTableName.class.getName());
            Object table = tableType.getConstructor(String.class, String.class, String.class)
                    .newInstance("lake", "schema", "items");
            Object scope = scopeType.getMethod("raw", tableType, Set.class, tableType)
                    .invoke(null, table, Set.of("id"), table);
            Class<?> specType = loader.loadClass(GenerationDispatchSpec.class.getName());
            Object spec = specType.getConstructor(String.class, graphType, String.class, String.class, scopeType,
                    long.class, int.class, long.class, long.class)
                    .newInstance("job", graph, "tenant", "principal", scope, 1L, 1, 1L, 1L);
            assertThat(specType.getMethod("stageGraph").invoke(spec)).isSameAs(graph);
            assertThat(specType.getMethod("authorizationScope").invoke(spec)).isSameAs(scope);
            requireStorageValuesWithoutJdbc(loader);
        }
    }

    private static List<String> contractNames(URL location, String prefix) throws Exception {
        var classes = java.nio.file.Path.of(location.toURI());
        List<String> names;
        if (java.nio.file.Files.isDirectory(classes)) {
            try (var files = java.nio.file.Files.walk(classes.resolve(prefix))) {
                names = files.filter(java.nio.file.Files::isRegularFile)
                        .map(classes::relativize).map(Object::toString).toList();
            }
        } else {
            try (var jar = new java.util.jar.JarFile(classes.toFile())) {
                names = jar.stream().map(java.util.jar.JarEntry::getName)
                        .filter(name -> name.startsWith(prefix + "/")).toList();
            }
        }
        var contracts = names.stream().filter(name -> name.endsWith(".class"))
                .filter(name -> !name.endsWith("package-info.class"))
                .map(name -> name.substring(0, name.length() - ".class".length()).replace('/', '.')).toList();
        assertThat(contracts).isNotEmpty();
        return contracts;
    }

    private static void requireStorageContractsWithoutJdbc(ClassLoader loader) throws Exception {
        List<Class<?>> records = new ArrayList<>();
        for (String name : List.of("execution.CurrentGenerationAuthority", "execution.GenerationStageExecutionStore",
                "exchange.GenerationStageExchangeStore", "job.GenerationJobStore",
                "publication.GenerationPublicationStore")) {
            var port = loader.loadClass("org.pangrova.persistence.generation.api." + name);
            assertThat(port.isInterface()).isTrue();
            assertThat(port.getClassLoader()).isSameAs(loader);
            assertThat(port.getMethods()).isNotEmpty();
            for (var method : port.getMethods()) {
                requireStorageSignatureWithoutJdbc(method.getGenericReturnType());
                for (var parameter : method.getGenericParameterTypes()) {
                    requireStorageSignatureWithoutJdbc(parameter);
                }
                for (var exception : method.getGenericExceptionTypes()) {
                    requireStorageSignatureWithoutJdbc(exception);
                }
            }
            records.addAll(List.of(port.getDeclaredClasses()));
        }
        assertThat(records).extracting(Class::getSimpleName).containsExactlyInAnyOrder(
                "PreparedExecution", "NativeWinner", "PartitionWindow", "PartitionClaim", "InputRange",
                "PreparedPartition", "ProducerCompletion", "PublishedInput", "ExchangeEvidence");
        for (Class<?> record : records) {
            assertThat(record.isRecord()).isTrue();
            assertThat(record.getClassLoader()).isSameAs(loader);
            assertThat(record.getConstructors()).hasSize(1);
            for (var component : record.getRecordComponents()) {
                requireStorageSignatureWithoutJdbc(component.getGenericType());
            }
        }
        var authority = loader.loadClass("org.pangrova.persistence.generation.api.execution.CurrentGenerationAuthority");
        assertThat(authority.getDeclaredMethods()).extracting(method -> method.getName())
                .containsExactlyInAnyOrder("dispatchSpec", "executionAnchor", "currentDirectRoles");
        for (var method : authority.getDeclaredMethods()) {
            assertThat(method.getParameterCount()).isZero();
        }
    }

    private static void requireStorageSignatureWithoutJdbc(Type type) {
        assertThat(type.getTypeName()).doesNotContain("java.sql.", "javax.sql.",
                "org.pangrova.persistence.jdbc.", "org.pangrova.persistence.generation.postgresql.",
                "org.pangrova.metadata.",
                "org.pangrova.coordinator.", "org.pangrova.query.store.",
                "org.pangrova.security.store.");
    }

    private static void requireStorageValuesWithoutJdbc(ClassLoader loader) throws Exception {
        var keyType = loader.loadClass("org.pangrova.generation.api.identity.GenerationJobKey");
        Object key = keyType.getConstructor(String.class, String.class).newInstance("tenant", "job");
        var anchorType = loader.loadClass("org.pangrova.execution.api.ownership.ExecutionAnchor");
        Instant now = Instant.parse("2026-10-01T00:00:00Z");
        Object anchor = anchorType.getConstructor(String.class, String.class, String.class, long.class,
                Instant.class, Instant.class).newInstance(keyType.getMethod("executionId").invoke(key),
                        "owner", "process", 1L, now, now.plusSeconds(60));
        var claimType = loader.loadClass(
                "org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore$PartitionClaim");
        var claimConstructor = claimType.getConstructor(keyType, anchorType, int.class, int.class, int.class,
                long.class, Duration.class, Instant.class);
        Object claim = claimConstructor.newInstance(key, anchor, 0, 0, 0, 1L, Duration.ofSeconds(30), now);
        assertThat(claimType.getMethod("anchor").invoke(claim)).isSameAs(anchor);
        assertThatThrownBy(() -> claimConstructor.newInstance(
                key, anchor, -1, 0, 0, 1L, Duration.ofSeconds(30), now))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> claimConstructor.newInstance(key, anchor, 0, 0, 0, 1L, Duration.ZERO, now))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);

        var ownerType = loader.loadClass("org.pangrova.generation.api.execution.GenerationExecutionOwner");
        Object owner = ownerType.getConstructor(String.class, String.class, long.class)
                .newInstance("owner", "process", 1L);
        var winnerType = loader.loadClass(
                "org.pangrova.persistence.generation.api.execution.GenerationStageExecutionStore$NativeWinner");
        byte[] wire = {1, 2, 3};
        Object winner = winnerType.getConstructor(String.class, int.class, long.class, long.class, ownerType,
                byte[].class, String.class).newInstance("page", 0, 1L, 1L, owner, wire,
                        Sha256Digests.sha256Hex(wire));
        wire[0] = 9;
        byte[] returned = (byte[]) winnerType.getMethod("originalWireBytes").invoke(winner);
        assertThat(returned).containsExactly(1, 2, 3);
        returned[1] = 9;
        assertThat((byte[]) winnerType.getMethod("originalWireBytes").invoke(winner)).containsExactly(1, 2, 3);
    }

    @Test
    void actualApiClasspathExcludesDomainRuntimeAndPostgresProvider() throws Exception {
        // This is the real test loader, with no filtering or hand-selected URLs.
        ClassLoader loader = GenerationStorageContractIsolationTest.class.getClassLoader();
        assertThat(Class.forName(GenerationDispatchSpec.class.getName(), false, loader))
                .isSameAs(GenerationDispatchSpec.class);
        for (String absent : List.of(
                "org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore",
                "org.pangrova.persistence.generation.postgresql.job.PostgresGenerationStageExecutionStore",
                "org.pangrova.persistence.generation.postgresql.exchange.PostgresGenerationStageExchangeStore",
                "org.pangrova.persistence.generation.postgresql.execution.PostgresGenerationExecutionFence",
                "org.pangrova.persistence.generation.postgresql.query.PostgresGenerationQueries")) {
            assertThatThrownBy(() -> Class.forName(absent, false, loader))
                    .as("Actual API classpath must not load %s", absent)
                    .isInstanceOf(ClassNotFoundException.class);
        }
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        assertThat(classpath).isNotBlank();
        for (String artifact : List.of("pangrova-generation-workflow", "pangrova-persistence-generation-postgresql", "pangrova-security-access", "pangrova-security-auth-builtin", "pangrova-security-auth-ldap",
                "pangrova-security-auth-oauth2", "pangrova-security-policy-builtin",
                "pangrova-security-secret-env", "pangrova-security-secret-file", "pangrova-security-kms-aws")) {
            assertThat(classpath).doesNotContain(artifact);
            assertThat(java.util.Collections.list(loader.getResources(
                    "META-INF/maven/org.pangrova/" + artifact + "/pom.properties"))).isEmpty();
        }
    }

}
