package org.pangrova.persistence.api.generation;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.generation.job.GenerationDispatchSpec;
import org.pangrova.spi.query.coordination.ExecutionAnchor;
import org.pangrova.spi.security.authz.DeferredAuthorizationScope;

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
                DigestUtils.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.persistence.api.generation.job.GenerationJobStore.class
                        .getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader())) {
            for (String missing : List.of("com.fasterxml.jackson.databind.ObjectMapper",
                    "org.pangrova.generation.assembly.GenerationStageGraphFactory",
                    "org.pangrova.generation.protocol.GenerationStageGraphCodec",
                    "org.pangrova.generation.protocol.GenerationExchangeEvidenceCodec",
                    "org.pangrova.persistence.jdbc.generation.job.PostgresGenerationJobStore",
                    "org.pangrova.persistence.jdbc.generation.job.PostgresGenerationStageExecutionStore",
                    "org.pangrova.persistence.jdbc.generation.exchange.PostgresGenerationStageExchangeStore",
                    "org.pangrova.persistence.jdbc.generation.execution.PostgresCurrentGenerationAuthority",
                    "org.pangrova.coordinator.bootstrap.CoordinatorBootstrap",
                    "org.pangrova.persistence.jdbc.query.coordination.JdbcExecutionAnchorStore",
                    "org.postgresql.Driver",
                    "org.pangrova.metadata.store.jdbc.catalog.MetadataJsonMapper")) {
                assertThatThrownBy(() -> loader.loadClass(missing)).isInstanceOf(ClassNotFoundException.class);
            }
            for (var entry : java.util.Map.of(locations[0], "org/pangrova/spi/generation",
                    locations[2], "org/pangrova/persistence/api/generation").entrySet()) {
                for (String name : contractNames(entry.getKey(), entry.getValue())) {
                    Class<?> type = loader.loadClass(name);
                    assertThat(type.getClassLoader()).isSameAs(loader);
                    type.getMethods();
                    type.getConstructors();
                    type.getRecordComponents();
                }
            }
            requireStorageContractsWithoutJdbc(loader);
            Class<?> graphType = loader.loadClass("org.pangrova.spi.generation.execution.GenerationStageGraph");
            Class<?> templateType = loader.loadClass(
                    "org.pangrova.spi.generation.execution.template.GenerationStageTemplate");
            Object template = templateType.getMethod("of", byte[].class, List.class, List.class)
                    .invoke(null, new byte[]{2}, List.of(), List.of());
            Class<?> kindType = loader.loadClass("org.pangrova.spi.generation.GenerationPageKind");
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
                "exchange.GenerationStageExchangeStore")) {
            var port = loader.loadClass("org.pangrova.persistence.api.generation." + name);
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
        var authority = loader.loadClass("org.pangrova.persistence.api.generation.execution.CurrentGenerationAuthority");
        assertThat(authority.getDeclaredMethods()).extracting(method -> method.getName())
                .containsExactlyInAnyOrder("dispatchSpec", "executionAnchor", "currentDirectRoles");
        for (var method : authority.getDeclaredMethods()) {
            assertThat(method.getParameterCount()).isZero();
        }
    }

    private static void requireStorageSignatureWithoutJdbc(Type type) {
        assertThat(type.getTypeName()).doesNotContain("java.sql.", "javax.sql.",
                "org.pangrova.persistence.jdbc.", "org.pangrova.metadata.",
                "org.pangrova.coordinator.", "org.pangrova.query.store.",
                "org.pangrova.security.store.");
    }

    private static void requireStorageValuesWithoutJdbc(ClassLoader loader) throws Exception {
        var keyType = loader.loadClass("org.pangrova.spi.generation.job.GenerationJobKey");
        Object key = keyType.getConstructor(String.class, String.class).newInstance("tenant", "job");
        var anchorType = loader.loadClass("org.pangrova.spi.query.coordination.ExecutionAnchor");
        Instant now = Instant.parse("2026-10-01T00:00:00Z");
        Object anchor = anchorType.getConstructor(String.class, String.class, String.class, long.class,
                Instant.class, Instant.class).newInstance(keyType.getMethod("executionId").invoke(key),
                        "owner", "process", 1L, now, now.plusSeconds(60));
        var claimType = loader.loadClass(
                "org.pangrova.persistence.api.generation.execution.GenerationStageExecutionStore$PartitionClaim");
        var claimConstructor = claimType.getConstructor(keyType, anchorType, int.class, int.class, int.class,
                long.class, Duration.class, Instant.class);
        Object claim = claimConstructor.newInstance(key, anchor, 0, 0, 0, 1L, Duration.ofSeconds(30), now);
        assertThat(claimType.getMethod("anchor").invoke(claim)).isSameAs(anchor);
        assertThatThrownBy(() -> claimConstructor.newInstance(
                key, anchor, -1, 0, 0, 1L, Duration.ofSeconds(30), now))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> claimConstructor.newInstance(key, anchor, 0, 0, 0, 1L, Duration.ZERO, now))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);

        var ownerType = loader.loadClass("org.pangrova.spi.generation.execution.GenerationExecutionOwner");
        Object owner = ownerType.getConstructor(String.class, String.class, long.class)
                .newInstance("owner", "process", 1L);
        var winnerType = loader.loadClass(
                "org.pangrova.persistence.api.generation.execution.GenerationStageExecutionStore$NativeWinner");
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
    void actualApiClasspathExcludesDomainRuntimeAndPostgresProvider() throws Exception {
        // This is the real test loader, with no filtering or hand-selected URLs.
        ClassLoader loader = GenerationStorageContractIsolationTest.class.getClassLoader();
        assertThat(Class.forName(GenerationDispatchSpec.class.getName(), false, loader))
                .isSameAs(GenerationDispatchSpec.class);
        for (String absent : List.of(
                "org.pangrova.persistence.jdbc.generation.job.PostgresGenerationJobStore",
                "org.pangrova.persistence.jdbc.generation.job.PostgresGenerationStageExecutionStore",
                "org.pangrova.persistence.jdbc.generation.exchange.PostgresGenerationStageExchangeStore",
                "org.pangrova.persistence.jdbc.generation.execution.PostgresGenerationExecutionFence",
                "org.pangrova.persistence.jdbc.generation.query.PostgresGenerationQueries")) {
            assertThatThrownBy(() -> Class.forName(absent, false, loader))
                    .as("Actual API classpath must not load %s", absent)
                    .isInstanceOf(ClassNotFoundException.class);
        }
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        assertThat(classpath).isNotBlank();
        for (String artifact : List.of("pangrova-generation-core", "pangrova-persistence-jdbc", "pangrova-security")) {
            assertThat(classpath).doesNotContain(artifact);
            assertThat(java.util.Collections.list(loader.getResources(
                    "META-INF/maven/org.pangrova/" + artifact + "/pom.properties"))).isEmpty();
        }
    }

}
