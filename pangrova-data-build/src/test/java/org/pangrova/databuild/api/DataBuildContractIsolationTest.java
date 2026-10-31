package org.pangrova.databuild.api;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
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

/** Constructs durable values while JDBC, codecs, JSON and domain runtime implementations are unavailable. */
class DataBuildContractIsolationTest {
    @Test
    void graphEvidenceAndDispatchValuesDoNotLinkSerializationOrStoreImplementations() throws Exception {
        URL[] locations = {
                DataBuildDispatchSpec.class.getProtectionDomain().getCodeSource().getLocation(),
                DigestUtils.class.getProtectionDomain().getCodeSource().getLocation(),
                QualifiedTableName.class.getProtectionDomain().getCodeSource().getLocation(),
                DeferredAuthorizationScope.class.getProtectionDomain().getCodeSource().getLocation(),
                ExecutionAnchor.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("org.pangrova.metadata.")
                        || (name.startsWith("org.pangrova.databuild.")
                        && !name.startsWith("org.pangrova.databuild.api.")
                        && !name.startsWith("org.pangrova.databuild.spi.storage."))
                        || (name.startsWith("org.pangrova.security.")
                        && !name.startsWith("org.pangrova.security.api."))) {
                    throw new ClassNotFoundException("Implementation unavailable: " + name);
                }
                return super.loadClass(name, resolve);
            }
        }) {
            for (String missing : List.of("com.fasterxml.jackson.databind.ObjectMapper",
                    "org.pangrova.databuild.assembly.DataBuildStageGraphFactory",
                    "org.pangrova.databuild.protocol.DataBuildStageGraphCodec",
                    "org.pangrova.databuild.protocol.DataBuildExchangeEvidenceCodec",
                    "org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStore",
                    "org.pangrova.databuild.store.jdbc.build.JdbcDataBuildStageExecutionStore",
                    "org.pangrova.databuild.store.jdbc.exchange.JdbcDataBuildStageExchangeStore",
                    "org.pangrova.databuild.store.jdbc.execution.CurrentDeferredExecutionContext",
                    "org.pangrova.coordinator.bootstrap.CoordinatorBootstrap",
                    "org.pangrova.query.store.jdbc.anchor.JdbcExecutionAnchorStore",
                    "org.postgresql.Driver",
                    "org.pangrova.metadata.store.jdbc.catalog.MetadataJsonMapper")) {
                assertThatThrownBy(() -> loader.loadClass(missing)).isInstanceOf(ClassNotFoundException.class);
            }
            Path classes = Path.of(locations[0].toURI());
            for (String contractPackage : List.of("api", "spi/storage")) {
                try (var files = Files.walk(classes.resolve("org/pangrova/databuild/" + contractPackage))) {
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
            Class<?> graphType = loader.loadClass("org.pangrova.databuild.api.execution.DataBuildStageGraph");
            Class<?> templateType = loader.loadClass(
                    "org.pangrova.databuild.api.execution.template.DataBuildStageTemplate");
            Object template = templateType.getMethod("of", byte[].class, List.class, List.class)
                    .invoke(null, new byte[]{2}, List.of(), List.of());
            Class<?> kindType = loader.loadClass("org.pangrova.databuild.api.DataBuildPageKind");
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
            Class<?> specType = loader.loadClass(DataBuildDispatchSpec.class.getName());
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
        for (String name : List.of("CurrentExecutionAuthority", "DataBuildStageExecutionStore",
                "DataBuildStageExchangeStore")) {
            var port = loader.loadClass("org.pangrova.databuild.spi.storage." + name);
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
        var authority = loader.loadClass("org.pangrova.databuild.spi.storage.CurrentExecutionAuthority");
        assertThat(authority.getDeclaredMethods()).extracting(method -> method.getName())
                .containsExactlyInAnyOrder("dispatchSpec", "executionAnchor", "currentDirectRoles");
        for (var method : authority.getDeclaredMethods()) {
            assertThat(method.getParameterCount()).isZero();
        }
    }

    private static void requireStorageSignatureWithoutJdbc(Type type) {
        assertThat(type.getTypeName()).doesNotContain("java.sql.", "javax.sql.",
                "org.pangrova.databuild.store.", "org.pangrova.metadata.",
                "org.pangrova.coordinator.", "org.pangrova.query.store.",
                "org.pangrova.security.store.");
    }

    private static void requireStorageValuesWithoutJdbc(ClassLoader loader) throws Exception {
        var keyType = loader.loadClass("org.pangrova.databuild.api.build.DataBuildJobKey");
        Object key = keyType.getConstructor(String.class, String.class).newInstance("tenant", "job");
        var anchorType = loader.loadClass("org.pangrova.query.api.coordination.ExecutionAnchor");
        Instant now = Instant.parse("2026-10-01T00:00:00Z");
        Object anchor = anchorType.getConstructor(String.class, String.class, String.class, long.class,
                Instant.class, Instant.class).newInstance(keyType.getMethod("executionId").invoke(key),
                        "owner", "process", 1L, now, now.plusSeconds(60));
        var claimType = loader.loadClass(
                "org.pangrova.databuild.spi.storage.DataBuildStageExecutionStore$PartitionClaim");
        var claimConstructor = claimType.getConstructor(keyType, anchorType, int.class, int.class, int.class,
                long.class, Duration.class, Instant.class);
        Object claim = claimConstructor.newInstance(key, anchor, 0, 0, 0, 1L, Duration.ofSeconds(30), now);
        assertThat(claimType.getMethod("anchor").invoke(claim)).isSameAs(anchor);
        assertThatThrownBy(() -> claimConstructor.newInstance(
                key, anchor, -1, 0, 0, 1L, Duration.ofSeconds(30), now))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> claimConstructor.newInstance(key, anchor, 0, 0, 0, 1L, Duration.ZERO, now))
                .hasRootCauseInstanceOf(IllegalArgumentException.class);

        var ownerType = loader.loadClass("org.pangrova.databuild.api.execution.DataBuildExecutionOwner");
        Object owner = ownerType.getConstructor(String.class, String.class, long.class)
                .newInstance("owner", "process", 1L);
        var winnerType = loader.loadClass(
                "org.pangrova.databuild.spi.storage.DataBuildStageExecutionStore$NativeWinner");
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
    void jdbcAuthorityPortLinksWithoutDomainImplementationsOrDatabaseDrivers() throws Exception {
        URL[] locations = {
                DataBuildDispatchSpec.class.getProtectionDomain().getCodeSource().getLocation(),
                DigestUtils.class.getProtectionDomain().getCodeSource().getLocation(),
                QualifiedTableName.class.getProtectionDomain().getCodeSource().getLocation(),
                DeferredAuthorizationScope.class.getProtectionDomain().getCodeSource().getLocation(),
                ExecutionAnchor.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(locations, ClassLoader.getPlatformClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.startsWith("org.pangrova.metadata.")
                        || name.startsWith("org.pangrova.databuild.store.")
                        || (name.startsWith("org.pangrova.security.")
                        && !name.startsWith("org.pangrova.security.api."))) {
                    throw new ClassNotFoundException("Domain implementation unavailable: " + name);
                }
                return super.loadClass(name, resolve);
            }
        }) {
            for (String name : List.of("DataBuildTransactionAuthority", "DataBuildPublicationCandidates",
                    "DataBuildJobSubmissionTransactions", "DataBuildExecutionTransactionFence",
                    "DataBuildTransactionInputs")) {
                var port = loader.loadClass("org.pangrova.databuild.spi.jdbc." + name);
                assertThat(port.isInterface()).isTrue();
                assertThat(port.getMethods()).isNotEmpty();
            }
            for (String missing : List.of("org.postgresql.Driver", "com.fasterxml.jackson.databind.ObjectMapper",
                    "org.pangrova.metadata.adapter.databuild.MetadataDataBuildTransactionAuthority")) {
                assertThatThrownBy(() -> loader.loadClass(missing)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }

}
