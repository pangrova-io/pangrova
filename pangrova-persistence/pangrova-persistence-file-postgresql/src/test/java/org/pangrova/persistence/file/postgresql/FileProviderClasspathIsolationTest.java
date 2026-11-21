package org.pangrova.persistence.file.postgresql;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Checks the real provider test classpath, without filtering away incorrect dependencies. */
class FileProviderClasspathIsolationTest {
    @Test
    void backendClassesLinkWithFlatFileArtifactAndWithoutServerRuntime() throws Exception {
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
        // File publishes its contracts and access service together. The manifest limits
        // this provider's actual imports within that artifact to org.pangrova.file.api.
        var fileContract = loader.loadClass("org.pangrova.file.api.space.FileSpace");
        var fileService = loader.loadClass("org.pangrova.file.access.FileUrlService");
        assertThat(fileService.getProtectionDomain().getCodeSource().getLocation())
                .isEqualTo(fileContract.getProtectionDomain().getCodeSource().getLocation());
        for (String name : List.of("org.pangrova.metadata.catalog.registry.CatalogRegistry",
                "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                "org.pangrova.query.runtime.QueryManager",
                "org.pangrova.worker.bootstrap.registration.WorkerRegistrationClient")) {
            assertThatThrownBy(() -> loader.loadClass(name)).as(name).isInstanceOf(ClassNotFoundException.class);
        }
    }
}
