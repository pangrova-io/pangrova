package org.pangrova.table.api;

import org.junit.jupiter.api.Test;
import org.pangrova.table.api.identity.TableRef;

import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks every Table contract on the real API dependency closure, without runtime libraries. */
class TableApiClasspathTest {
    @Test
    void valuesLinkWithoutStoragePortsCodecsOrDomainRuntime() throws Exception {
        var loader = getClass().getClassLoader();
        Path classes = Path.of(TableRef.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        try (var paths = Files.walk(classes.resolve("org/pangrova/table/api"))) {
            var files = paths.filter(path -> path.toString().endsWith(".class")).toList();
            assertThat(files).isNotEmpty();
            for (Path path : files) {
                String name = classes.relativize(path).toString()
                        .replace(path.getFileSystem().getSeparator(), ".");
                Class<?> type = Class.forName(name.substring(0, name.length() - 6), true, loader);
                type.getGenericSuperclass();
                type.getGenericInterfaces();
                type.getAnnotations();
                type.getRecordComponents();
                for (var method : type.getDeclaredMethods()) {
                    method.getGenericReturnType();
                    method.getGenericParameterTypes();
                    method.getGenericExceptionTypes();
                    method.getAnnotations();
                }
                for (var constructor : type.getDeclaredConstructors()) {
                    constructor.getGenericParameterTypes();
                    constructor.getGenericExceptionTypes();
                }
                for (var field : type.getDeclaredFields()) field.getGenericType();
            }
        }
        assertThat(new TableRef("catalog", "schema", "table")).isNotNull();
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        assertThat(classpath).isNotBlank();
        for (String artifact : List.of("pangrova-spi", "pangrova-persistence-generation-api",
                "pangrova-persistence-generation-postgresql", "pangrova-coordination-control", "pangrova-worker")) {
            assertThat(classpath).doesNotContain(artifact);
            assertThat(java.util.Collections.list(loader.getResources(
                    "META-INF/maven/org.pangrova/" + artifact + "/pom.properties"))).isEmpty();
        }
        for (String name : List.of(
                "org.pangrova.metadata.catalog.registry.CatalogRegistry",
                "org.pangrova.persistence.table.postgresql.control.JdbcTableControlStore",
                "org.pangrova.security.authorization.deferred.DeferredAuthorization",
                "org.pangrova.protocol.v2.TaskRequest",
                "org.apache.arrow.vector.VectorSchemaRoot",
                "com.fasterxml.jackson.databind.ObjectMapper",
                "org.apache.calcite.rel.type.RelDataType",
                "org.postgresql.Driver")) {
            assertThatThrownBy(() -> Class.forName(name, false, loader))
                    .as("Table API must not load %s", name).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
