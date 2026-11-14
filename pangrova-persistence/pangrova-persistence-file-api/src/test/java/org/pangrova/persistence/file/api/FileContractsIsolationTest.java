package org.pangrova.persistence.file.api;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.file.api.space.FileSpaceRegistry;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileContractsIsolationTest {
    @Test
    void persistenceContractsLoadWithOnlyTheDomainContractClosure() throws Exception {
        URL[] artifacts = {
                org.pangrova.execution.api.ownership.ExecutionAnchor.class.getProtectionDomain().getCodeSource().getLocation(),
                FileSpaceRegistry.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.file.api.space.FileSpace.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.filesystem.api.profile.StorageProfile.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.security.api.identity.UserIdentity.class.getProtectionDomain().getCodeSource().getLocation(),
                org.pangrova.common.exception.PangrovaException.class.getProtectionDomain().getCodeSource().getLocation()
        };
        try (var loader = new URLClassLoader(artifacts, ClassLoader.getPlatformClassLoader())) {
            for (String name : List.of(
                    "org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions",
                    "org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions$Ticket",
                    "org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions$Scope",
                    "org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions$StagedSource",
                    "org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions$Redemption",
                    "org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions$FrozenInputAttempt",
                    "org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions$FrozenInput",
                    "org.pangrova.persistence.file.api.space.FileSpaceRegistry",
                    "org.pangrova.persistence.file.api.profile.StorageProfileRegistry",
                    "org.pangrova.file.api.profile.StorageProfileRevision",
                    "org.pangrova.persistence.file.api.directory.FileDirectoryIndex",
                    "org.pangrova.file.api.directory.DirectoryRefresh$RefreshToken",
                    "org.pangrova.persistence.file.api.claim.CopyFileClaim",
                    "org.pangrova.persistence.file.api.claim.CopyFileClaimStore",
                    "org.pangrova.persistence.file.api.enums.CopyFileClaimResult")) {
                var type = Class.forName(name, true, loader);
                assertThat(type.getClassLoader()).isSameAs(loader);
                for (var method : type.getMethods()) {
                    method.getGenericReturnType();
                    method.getGenericParameterTypes();
                    method.getGenericExceptionTypes();
                }
                type.getConstructors();
                type.getRecordComponents();
            }
            var ticketType = loader.loadClass(
                    "org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions$Ticket");
            var ticket = ticketType.getConstructor(String.class).newInstance("A".repeat(43));
            assertThat(ticket.toString()).doesNotContain("A".repeat(43));
            assertThatThrownBy(() -> ticketType.getConstructor(String.class).newInstance("short"))
                    .hasCauseInstanceOf(IllegalArgumentException.class);
            var sourceType = loader.loadClass(
                    "org.pangrova.persistence.file.api.copy.CopySourceReadAdmissions$StagedSource");
            var sourceConstructor = sourceType.getConstructor(String.class, String.class,
                    String.class, long.class, String.class);
            String checksum = "a".repeat(64);
            String tenant = "b".repeat(64);
            String uri = "s3://managed/copy/tenants/" + tenant + "/sha256/aa/" + checksum;
            var source = sourceConstructor.newInstance("version-1", uri, checksum, 1L, tenant);
            assertThat(sourceType.getMethod("managedObjectUri").invoke(source)).isEqualTo(uri);
            assertThatThrownBy(() -> sourceConstructor.newInstance(
                    "version-1", uri.replace("/copy/", "/copy/../"), checksum, 1L, tenant))
                    .hasCauseInstanceOf(IllegalArgumentException.class);
            for (String name : List.of("org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry",
                    "org.pangrova.metadata.catalog.registry.CatalogRegistry",
                    "org.pangrova.testing.testkit.file.InMemoryStorageProfileRegistry",
                    "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap",
                    "com.fasterxml.jackson.databind.ObjectMapper", "org.postgresql.Driver")) {
                assertThatThrownBy(() -> loader.loadClass(name)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }
}
