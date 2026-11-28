package org.pangrova.testing.testkit;

import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.pangrova.common.concurrency.NotThreadSafe;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;

/** Copies bounded startup material to the real non-root process user before that process starts. */
@NotThreadSafe
final class RuntimeUserContainer extends GenericContainer<RuntimeUserContainer> {
    // A startup-material limit, not a data-file transfer or process-memory grant.
    static final int MAX_STARTUP_ARCHIVE_BYTES = 1024 * 1024;

    RuntimeUserContainer(DockerImageName image) {
        super(image);
    }

    @Override
    public void copyFileToContainer(Transferable material, String destination) {
        String id = getContainerId();
        if (id == null) {
            throw new IllegalStateException("Startup copying requires its created container owner");
        }
        var config = getDockerClient().inspectContainerCmd(id).exec().getConfig();
        requireNonRootUser(config == null ? null : config.getUser());
        var archive = new StartupArchive();
        try {
            try (var output = new TarArchiveOutputStream(archive)) {
                output.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
                output.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
                material.transferTo(output, destination);
                output.finish();
            }
            try (var input = archive.openInput()) {
                getDockerClient().copyArchiveToContainerCmd(id).withTarInputStream(input)
                        .withRemotePath("/").withCopyUIDGID(true).withNoOverwriteDirNonDir(true).exec();
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("Startup material could not be copied to its runtime user", failure);
        } finally {
            archive.erase();
        }
    }

    private static void requireNonRootUser(String configured) {
        if (configured == null || configured.isBlank()) {
            throw new IllegalArgumentException("Startup copying requires an explicit non-root runtime user");
        }
        String user = configured.split(":", -1)[0];
        if (user.isBlank() || user.equals("root") || user.matches("0+")) {
            throw new IllegalArgumentException("Startup copying refuses the root runtime user");
        }
    }

    /** Owns the actual tar bytes until the synchronous Docker upload returns, then erases its entire buffer. */
    static final class StartupArchive extends ByteArrayOutputStream {
        StartupArchive() {
            super(MAX_STARTUP_ARCHIVE_BYTES);
        }

        @Override
        public synchronized void write(int value) {
            requireCapacity(1);
            super.write(value);
        }

        @Override
        public synchronized void write(byte[] bytes, int offset, int length) {
            Objects.checkFromIndexSize(offset, length, bytes.length);
            requireCapacity(length);
            super.write(bytes, offset, length);
        }

        private void requireCapacity(int additional) {
            if (additional > MAX_STARTUP_ARCHIVE_BYTES - count) {
                throw new IllegalArgumentException("Startup archive exceeds its exact byte limit");
            }
        }

        synchronized ByteArrayInputStream openInput() {
            return new ByteArrayInputStream(buf, 0, count);
        }

        synchronized void erase() {
            Arrays.fill(buf, (byte) 0);
            reset();
        }
    }
}
