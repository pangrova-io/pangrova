package org.pangrova.databuild.api;

import org.junit.jupiter.api.Test;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;
import org.pangrova.databuild.api.build.DataBuildDispatchSpec;
import org.pangrova.query.api.coordination.ExecutionAnchor;
import org.pangrova.security.api.authz.DeferredAuthorizationScope;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
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
                        && !name.startsWith("org.pangrova.databuild.api."))
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
                    "org.pangrova.metadata.store.jdbc.catalog.MetadataJsonMapper")) {
                assertThatThrownBy(() -> loader.loadClass(missing)).isInstanceOf(ClassNotFoundException.class);
            }
            Path classes = Path.of(locations[0].toURI());
            try (var files = Files.walk(classes.resolve("org/pangrova/databuild/api"))) {
                List<Path> contracts = files.filter(path -> path.toString().endsWith(".class"))
                        .filter(path -> !path.getFileName().toString().equals("package-info.class")).toList();
                assertThat(contracts).isNotEmpty();
                for (Path contract : contracts) {
                    String name = classes.relativize(contract).toString().replace('/', '.');
                    Class<?> type = loader.loadClass(name.substring(0, name.length() - ".class".length()));
                    type.getMethods();
                    type.getConstructors();
                    type.getRecordComponents();
                }
            }
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
        }
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
            for (String name : List.of("DataBuildTransactionAuthority", "DataBuildPublicationCandidates")) {
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
