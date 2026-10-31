package org.pangrova.generation.api;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.generation.api.job.GenerationDispatchSpec;
import org.pangrova.query.api.coordination.ExecutionAnchor;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;

import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies isolated value contracts and the real core classpath boundary. */
class GenerationContractIsolationTest {
    @Test
    void graphEvidenceAndDispatchValuesDoNotLinkSerializationOrStoreImplementations() throws Exception {
        URL[] locations = {
                GenerationDispatchSpec.class.getProtectionDomain().getCodeSource().getLocation(),
                DigestUtils.class.getProtectionDomain().getCodeSource().getLocation(),
                QualifiedTableName.class.getProtectionDomain().getCodeSource().getLocation(),
                DeferredAuthorizationScope.class.getProtectionDomain().getCodeSource().getLocation(),
                ExecutionAnchor.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("org.pangrova.metadata.")
                        || (name.startsWith("org.pangrova.generation.")
                        && !name.startsWith("org.pangrova.generation.api.")
                        && !name.startsWith("org.pangrova.generation.spi.storage."))
                        || (name.startsWith("org.pangrova.security.")
                        && !name.startsWith("org.pangrova.security.api."))) {
                    throw new ClassNotFoundException("Implementation unavailable: " + name);
                }
                return super.loadClass(name, resolve);
            }
        }) {
            for (String missing : List.of("com.fasterxml.jackson.databind.ObjectMapper",
                    "org.pangrova.generation.assembly.GenerationStageGraphFactory",
                    "org.pangrova.generation.protocol.GenerationStageGraphCodec",
                    "org.pangrova.generation.protocol.GenerationExchangeEvidenceCodec",
                    "org.pangrova.generation.store.postgresql.job.PostgresGenerationJobStore",
                    "org.pangrova.generation.store.postgresql.job.PostgresGenerationStageExecutionStore",
                    "org.pangrova.generation.store.postgresql.exchange.PostgresGenerationStageExchangeStore",
                    "org.pangrova.generation.store.postgresql.execution.PostgresCurrentGenerationAuthority",
                    "org.pangrova.coordinator.bootstrap.CoordinatorBootstrap",
                    "org.pangrova.query.store.jdbc.anchor.JdbcExecutionAnchorStore",
                    "org.postgresql.Driver",
                    "org.pangrova.metadata.store.jdbc.catalog.MetadataJsonMapper")) {
                assertThatThrownBy(() -> loader.loadClass(missing)).isInstanceOf(ClassNotFoundException.class);
            }
            Path classes = Path.of(locations[0].toURI());
            for (String contractPackage : List.of("api", "spi/storage")) {
                try (var files = Files.walk(classes.resolve("org/pangrova/generation/" + contractPackage))) {
                    List<Path> contracts = files.filter(path -> path.toString().endsWith(".class"))
                            .filter(path -> !path.getFileName().toString().equals("package-info.class")).toList();
                    assertThat(contracts).isNotEmpty();
                    for (Path contract : contracts) {
                        String name = classes.relativize(contract).toString().replace('/', '.');
                        Class<?> type = loader.loadClass(name.substring(0, name.length() - ".class".length()));
                        assertThat(type.getClassLoader()).isSameAs(loader);
                        type.getMethods();
                        type.getConstructors();
                        type.getRecordComponents();
                    }
                }
            }
            requireStorageContractsWithoutJdbc(loader);
            Class<?> graphType = loader.loadClass("org.pangrova.generation.api.execution.GenerationStageGraph");
            Class<?> templateType = loader.loadClass(
                    "org.pangrova.generation.api.execution.template.GenerationStageTemplate");
            Object template = templateType.getMethod("of", byte[].class, List.class, List.class)
                    .invoke(null, new byte[]{2}, List.of(), List.of());
            Class<?> kindType = loader.loadClass("org.pangrova.generation.api.GenerationPageKind");
            Class<?> stageType = loader.loadClass(graphType.getName() + "$StagePlan");
            Object stage = stageType.getMethod("of", int.class, int.class, kindType, byte[].class,
                            templateType, List.class, List.class, boolean.class)
                    .invoke(null, 0, 0, kindType.getField("SOURCE_MAP").get(null), new byte[]{1},
                            template, List.of(), List.of(), true);
            Class<?> purposeType = loader.loadClass(graphType.getName() + "$Purpose");
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

    private static void requireStorageContractsWithoutJdbc(ClassLoader loader) throws Exception {
        List<Class<?>> records = new ArrayList<>();
        for (String name : List.of("CurrentGenerationAuthority", "GenerationStageExecutionStore",
                "GenerationStageExchangeStore")) {
            var port = loader.loadClass("org.pangrova.generation.spi.storage." + name);
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
                "PreparedExecution", "NativeWinner", "PartitionProgress", "PartitionClaim",
                "PreparedPartition", "ProducerCompletion", "PublishedInput", "ExchangeEvidence");
        for (Class<?> record : records) {
            assertThat(record.isRecord()).isTrue();
            assertThat(record.getClassLoader()).isSameAs(loader);
            assertThat(record.getConstructors()).hasSize(1);
            for (var component : record.getRecordComponents()) {
                requireStorageSignatureWithoutJdbc(component.getGenericType());
            }
        }
        var authority = loader.loadClass("org.pangrova.generation.spi.storage.CurrentGenerationAuthority");
        assertThat(authority.getDeclaredMethods()).extracting(method -> method.getName())
                .containsExactlyInAnyOrder("dispatchSpec", "executionAnchor", "currentDirectRoles");
        for (var method : authority.getDeclaredMethods()) {
            assertThat(method.getParameterCount()).isZero();
        }
    }

    private static void requireStorageSignatureWithoutJdbc(Type type) {
        assertThat(type.getTypeName()).doesNotContain("java.sql.", "javax.sql.",
                "org.pangrova.generation.store.", "org.pangrova.metadata.",
                "org.pangrova.coordinator.", "org.pangrova.query.store.",
                "org.pangrova.security.store.");
    }

    private static void requireStorageValuesWithoutJdbc(ClassLoader loader) throws Exception {
        var keyType = loader.loadClass("org.pangrova.generation.api.job.GenerationJobKey");
        Object key = keyType.getConstructor(String.class, String.class).newInstance("tenant", "job");
        var anchorType = loader.loadClass("org.pangrova.query.api.coordination.ExecutionAnchor");
        Instant now = Instant.parse("2026-10-01T00:00:00Z");
        Object anchor = anchorType.getConstructor(String.class, String.class, String.class, long.class,
                Instant.class, Instant.class).newInstance(keyType.getMethod("executionId").invoke(key),
                        "owner", "process", 1L, now, now.plusSeconds(60));
        var claimType = loader.loadClass(
                "org.pangrova.generation.spi.storage.GenerationStageExecutionStore$PartitionClaim");
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
                "org.pangrova.generation.spi.storage.GenerationStageExecutionStore$NativeWinner");
        byte[] wire = {1, 2, 3};
        Object winner = winnerType.getConstructor(String.class, int.class, long.class, long.class, ownerType,
                byte[].class, String.class).newInstance("page", 0, 1L, 1L, owner, wire,
                        DigestUtils.sha256Hex(wire));
        wire[0] = 9;
        byte[] returned = (byte[]) winnerType.getMethod("originalWireBytes").invoke(winner);
        assertThat(returned).containsExactly(1, 2, 3);
        returned[1] = 9;
        assertThat((byte[]) winnerType.getMethod("originalWireBytes").invoke(winner)).containsExactly(1, 2, 3);
    }

    @Test
    void actualCoreTestClasspathExcludesTransactionSpiAndPostgresProvider() throws Exception {
        // This is the real test loader, with no filtering or hand-selected URLs.
        ClassLoader loader = GenerationContractIsolationTest.class.getClassLoader();
        assertThat(Class.forName(GenerationDispatchSpec.class.getName(), false, loader))
                .isSameAs(GenerationDispatchSpec.class);
        for (String absent : List.of(
                "org.pangrova.generation.spi.jdbc.GenerationTransactionAuthority",
                "org.pangrova.generation.spi.jdbc.GenerationPublicationCandidates",
                "org.pangrova.generation.spi.jdbc.GenerationJobSubmissionTransactions",
                "org.pangrova.generation.spi.jdbc.GenerationExecutionTransactionFence",
                "org.pangrova.generation.spi.jdbc.GenerationTransactionInputs",
                "org.pangrova.generation.store.postgresql.job.PostgresGenerationJobStore",
                "org.pangrova.generation.store.postgresql.job.PostgresGenerationStageExecutionStore",
                "org.pangrova.generation.store.postgresql.exchange.PostgresGenerationStageExchangeStore",
                "org.pangrova.generation.store.postgresql.execution.PostgresGenerationExecutionFence",
                "org.pangrova.generation.store.postgresql.query.PostgresGenerationQueries")) {
            assertThatThrownBy(() -> Class.forName(absent, false, loader))
                    .as("Actual core classpath must not load %s", absent)
                    .isInstanceOf(ClassNotFoundException.class);
        }
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        assertThat(classpath).isNotBlank();
        for (String artifact : List.of("pangrova-generation-transaction-spi", "pangrova-generation-store-postgresql")) {
            assertThat(classpath).doesNotContain(artifact);
            assertThat(java.util.Collections.list(loader.getResources(
                    "META-INF/maven/org.pangrova/" + artifact + "/pom.properties"))).isEmpty();
        }
    }

}
