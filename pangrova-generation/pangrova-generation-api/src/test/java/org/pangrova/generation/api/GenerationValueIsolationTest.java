package org.pangrova.generation.api;

import org.junit.jupiter.api.Test;
import org.pangrova.generation.api.identity.GenerationJobKey;

import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks every Generation contract on the real API dependency closure, without runtime libraries. */
class GenerationValueIsolationTest {
    @Test
    void valuesLinkWithoutStoragePortsCodecsOrDomainRuntime() throws Exception {
        var loader = getClass().getClassLoader();
        Path classes = Path.of(GenerationJobKey.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        try (var paths = Files.walk(classes.resolve("org/pangrova/generation/api"))) {
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
        assertThat(new GenerationJobKey("tenant", "job").executionId()).isNotBlank();
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        assertThat(classpath).isNotBlank();
        for (String artifact : List.of("pangrova-spi", "pangrova-generation-workflow", "pangrova-generation-codec",
                "pangrova-persistence-generation-postgresql", "pangrova-coordination-control", "pangrova-worker")) {
            assertThat(classpath).doesNotContain(artifact);
            assertThat(java.util.Collections.list(loader.getResources(
                    "META-INF/maven/org.pangrova/" + artifact + "/pom.properties"))).isEmpty();
        }
        for (String name : List.of(
                "org.pangrova.persistence.generation.api.job.GenerationJobStore",
                "org.pangrova.persistence.jdbc.api.generation.GenerationTransactionAuthority",
                "org.pangrova.persistence.generation.postgresql.job.PostgresGenerationJobStore",
                "org.pangrova.generation.codec.execution.GenerationStageGraphCodec",
                "org.pangrova.generation.submission.GenerationStageGraphFactory",
                "org.pangrova.security.authorization.deferred.DeferredAuthorization",
                "org.pangrova.generation.codec.format.BuiltInGenerationOutputFormats",
                "org.pangrova.coordinator.generation.DurableGenerationDispatcher",
                "org.pangrova.protocol.v2.TaskRequest",
                "org.apache.arrow.vector.VectorSchemaRoot",
                "com.fasterxml.jackson.databind.ObjectMapper",
                "org.apache.calcite.rel.type.RelDataType",
                "org.postgresql.Driver")) {
            assertThatThrownBy(() -> Class.forName(name, false, loader))
                    .as("Generation API must not load %s", name).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
