package org.pangrova.persistence.indexing.api.metadata;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexPersistenceApiIsolationTest {
    @Test
    void everyContractLoadsWithoutCoreProvidersJdbcDriversOrCodecs() throws Exception {
        for (String type : List.of("org.pangrova.indexing.build.IndexBuildCoordinator",
                "org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore",
                "org.pangrova.metadata.service.TableControlDeferredPolicy",
                "org.pangrova.indexing.codec.data.IndexSerializer",
                "com.fasterxml.jackson.databind.ObjectMapper", "org.postgresql.Driver",
                "com.zaxxer.hikari.HikariDataSource")) {
            assertThatThrownBy(() -> Class.forName(type)).isInstanceOf(ClassNotFoundException.class);
        }
        Path root = Path.of(IndexMetadataStore.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        try (var files = Files.walk(root.resolve("org/pangrova/persistence/indexing/api"))) {
            var names = files.filter(p -> p.toString().endsWith(".class"))
                    .map(p -> root.relativize(p).toString().replace(java.io.File.separatorChar, '.')
                            .replaceFirst("\\.class$", "")).toList();
            assertThat(names).isNotEmpty();
            for (String name : names) {
                Class<?> type = Class.forName(name);
                type.getGenericInterfaces();
                for (var method : type.getDeclaredMethods()) {
                    method.getGenericReturnType();
                    method.getGenericParameterTypes();
                    method.getGenericExceptionTypes();
                }
                for (var field : type.getDeclaredFields()) field.getGenericType();
                for (var constructor : type.getDeclaredConstructors()) constructor.getGenericParameterTypes();
                type.getRecordComponents();
            }
        }
    }
}
