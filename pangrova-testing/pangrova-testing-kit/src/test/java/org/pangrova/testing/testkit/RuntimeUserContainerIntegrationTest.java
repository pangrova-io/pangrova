package org.pangrova.testing.testkit;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.testing.testkit.security.TestTlsAuthority;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual Docker file ownership only; does not assert PostgreSQL service or Pangrova execution. */
class RuntimeUserContainerIntegrationTest extends ContainerTestBase {
    private static final String DIRECTORY = "/tmp/role-tls";

    @TempDir
    Path workspace;

    @Test
    void copiesOnlyTheSelectedIdentityForTheActualNonRootUser() throws Exception {
        try (var authority = new TestTlsAuthority(workspace)) {
            var selected = authority.issue("worker-1", "localhost", false);
            var other = authority.issue("worker-2", "localhost", false);
            try (var container = process("postgres")
                    .withCopyToContainer(directory(), DIRECTORY)
                    .withCopyToContainer(MountableFile.forHostPath(authority.root(), 0444), DIRECTORY + "/root.pem")
                    .withCopyToContainer(MountableFile.forHostPath(selected.certificateChain(), 0444),
                            DIRECTORY + "/identity.pem")
                    .withCopyToContainer(MountableFile.forHostPath(selected.privateKey(), 0400),
                            DIRECTORY + "/identity-key.pem")) {
                container.start();
                String uid = successful(container, "id", "-u").trim();
                String gid = successful(container, "id", "-g").trim();
                assertThat(uid).isNotEqualTo("0");
                assertThat(successful(container, "stat", "-c", "%a:%u:%g", DIRECTORY).trim())
                        .isEqualTo("700:" + uid + ":" + gid);
                assertThat(successful(container, "stat", "-c", "%a:%u:%g", DIRECTORY + "/identity-key.pem").trim())
                        .isEqualTo("400:" + uid + ":" + gid);
                assertThat(container.execInContainer("test", "-r", DIRECTORY + "/identity-key.pem").getExitCode())
                        .isZero();
                assertThat(successful(container, "head", "-n", "1", DIRECTORY + "/identity-key.pem"))
                        .isEqualTo("-----BEGIN PRIVATE KEY-----\n");
                try (var copied = container.getDockerClient().copyArchiveFromContainerCmd(
                        container.getContainerId(), DIRECTORY).exec();
                        var archive = new org.apache.commons.compress.archivers.tar.TarArchiveInputStream(copied)) {
                    var names = new java.util.ArrayList<String>();
                    org.apache.commons.compress.archivers.ArchiveEntry entry;
                    while ((entry = archive.getNextEntry()) != null) names.add(entry.getName());
                    assertThat(names).containsExactlyInAnyOrder("role-tls/", "role-tls/root.pem",
                            "role-tls/identity.pem", "role-tls/identity-key.pem");
                }
                assertThat(container.execInContainer("test", "-e", other.privateKey().toString()).getExitCode())
                        .isNotZero();
                assertThat(container.execInContainer("test", "-e",
                        authority.root().getParent().resolve("identities.p12").toString()).getExitCode()).isNotZero();
            }
        }
    }

    @Test
    void rejectsTheConfiguredRootUserBeforeStartingTheProcess() {
        try (var container = process("root").withCopyToContainer(Transferable.of("owned"), "/tmp/material")) {
            assertThatThrownBy(container::start).hasStackTraceContaining("refuses the root runtime user");
        }
    }

    @Test
    void rejectsAnUnspecifiedRuntimeUserBeforeStartingTheProcess() {
        try (var container = process("").withCopyToContainer(Transferable.of("owned"), "/tmp/material")) {
            assertThatThrownBy(container::start).hasStackTraceContaining("explicit non-root runtime user");
        }
    }

    @Test
    void rejectsTheActualOversizedArchiveBeforeStartingTheProcess() {
        byte[] bytes = new byte[RuntimeUserContainer.MAX_STARTUP_ARCHIVE_BYTES + 1];
        try (var container = process("postgres").withCopyToContainer(Transferable.of(bytes), "/tmp/material")) {
            assertThatThrownBy(container::start)
                    .hasStackTraceContaining("Startup archive exceeds its exact byte limit");
        }
    }

    private static RuntimeUserContainer process(String user) {
        return new RuntimeUserContainer(DockerImageName.parse("postgres:16-alpine"))
                .withCreateContainerCmdModifier(command -> command.withUser(user))
                .withCommand("tail", "-f", "/dev/null")
                .waitingFor(Wait.forSuccessfulCommand("id -u"));
    }

    private static String successful(RuntimeUserContainer container, String... arguments) throws Exception {
        var result = container.execInContainer(arguments);
        assertThat(result.getExitCode()).as("Actual non-root command exit").isZero();
        assertThat(result.getStderr()).isEmpty();
        return result.getStdout();
    }

    private static Transferable directory() {
        return new Transferable() {
            @Override public long getSize() { return 0; }

            @Override
            public void transferTo(TarArchiveOutputStream output, String destination) {
                try {
                    var entry = new TarArchiveEntry(destination + "/");
                    entry.setMode(0700);
                    output.putArchiveEntry(entry);
                    output.closeArchiveEntry();
                } catch (IOException failure) {
                    throw new UncheckedIOException(failure);
                }
            }
        };
    }
}
