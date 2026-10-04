package org.pangrova.testing.testkit.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTlsAuthorityPermissionsTest {
    @TempDir
    Path workspace;

    @Test
    void privateAuthorityAndIssuedKeysAreOwnerOnlyAndRetiredOnClose() throws Exception {
        Path directory;
        try (var authority = new TestTlsAuthority(workspace)) {
            directory = authority.root().getParent();
            var identity = authority.issue("worker-1", "localhost", false);
            assertThat(Files.getPosixFilePermissions(directory))
                    .isEqualTo(PosixFilePermissions.fromString("rwx------"));
            assertThat(Files.getPosixFilePermissions(directory.resolve("identities.p12")))
                    .isEqualTo(PosixFilePermissions.fromString("rw-------"));
            assertThat(Files.getPosixFilePermissions(identity.privateKey()))
                    .isEqualTo(PosixFilePermissions.fromString("rw-------"));
            assertThat(Files.readString(identity.privateKey())).startsWith("-----BEGIN PRIVATE KEY-----");
        }
        assertThat(directory).doesNotExist();
    }

    @Test
    void shellAndKeytoolOptionInjectionCannotCreateAnIdentity() throws Exception {
        try (var authority = new TestTlsAuthority(workspace)) {
            for (String name : new String[] {"worker;touch marker", "../worker", "worker -importcert", "$(id)"}) {
                assertThatThrownBy(() -> authority.issue(name, "localhost", false))
                        .isInstanceOf(IllegalArgumentException.class);
            }
            for (String hostname : new String[] {"localhost,IP:127.0.0.1", "$(id)", "localhost;id", "localhost -ext"}) {
                assertThatThrownBy(() -> authority.issue("worker-1", hostname, false))
                        .isInstanceOf(IllegalArgumentException.class);
            }
            try (var paths = Files.list(authority.root().getParent())) {
                assertThat(paths.map(path -> path.getFileName().toString()).toList())
                        .doesNotContain("worker-1-key.pem", "worker-1.pem", "marker");
            }
        }
    }
}
