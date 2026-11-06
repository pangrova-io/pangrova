package org.pangrova.persistence.file.postgresql;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks the real provider test classpath, without filtering away incorrect dependencies. */
class FileProviderClasspathIsolationTest {
    @Test
    void backendClassesLinkWithoutDomainCoreOrServerRuntime() throws Exception {
        var loader = getClass().getClassLoader();
        for (String name : List.of(
                "org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry",
                "org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceAuthority",
                "org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry",
                "org.pangrova.persistence.file.postgresql.directory.JdbcFileDirectoryIndex",
                "org.pangrova.persistence.file.postgresql.claim.JdbcCopyFileClaimStore",
                "org.pangrova.persistence.file.postgresql.copy.JdbcCopySourceReadAdmissions")) {
            var type = Class.forName(name, true, loader);
            assertThat(type.getDeclaredConstructors()).isNotEmpty();
            type.getMethods();
            for (var nested : type.getDeclaredClasses()) {
                nested.getDeclaredMethods();
                nested.getDeclaredConstructors();
            }
        }
        for (String name : List.of("org.pangrova.file.core.access.FileUrlService",
                "org.pangrova.metadata.catalog.registry.CatalogRegistry",
                "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.coordinator.query.QueryManager")) {
            assertThatThrownBy(() -> loader.loadClass(name)).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
