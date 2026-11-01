package org.pangrova.filesystem.local.object;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.filesystem.api.object.FileStat;

/** Owns one private, forced publication candidate and its byte-derived immutable receipt. */
final class LocalObjectDraft implements AutoCloseable {
    private static final int BUFFER_BYTES = 64 * 1024;
    private final Path path;
    private final FileChannel channel;
    private final MessageDigest digest = sha256();
    private final OutputStream output;

    private LocalObjectDraft(Path path, FileChannel channel) {
        this.path = path;
        this.channel = channel;
        this.output = new DigestOutputStream(Channels.newOutputStream(channel), digest);
    }

    static LocalObjectDraft create(Path parent) throws IOException {
        Path path = Files.createTempFile(parent, ".pangrova-upload-", ".part");
        FileChannel channel = null;
        try {
            channel = FileChannel.open(path, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING, LinkOption.NOFOLLOW_LINKS);
            return new LocalObjectDraft(path, channel);
        } catch (IOException | RuntimeException | Error failure) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException | RuntimeException | Error cleanup) {
                    if (cleanup != failure) failure.addSuppressed(cleanup);
                }
            }
            try {
                Files.deleteIfExists(path);
            } catch (IOException | RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    Path path() { return path; }
    OutputStream output() { return output; }

    long copy(InputStream input, long expected) throws IOException {
        long written = 0;
        byte[] bytes = new byte[BUFFER_BYTES];
        int count;
        while ((count = input.read(bytes)) != -1) {
            written = Math.addExact(written, count);
            if (expected >= 0 && written > expected) {
                throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                        "Upload exceeds the declared size");
            }
            output.write(bytes, 0, count);
        }
        if (expected >= 0 && written != expected) {
            throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                    "Uploaded bytes do not match the declared size");
        }
        return written;
    }

    Content seal() throws IOException {
        output.flush();
        channel.force(true);
        long size = channel.size();
        output.close();
        return new Content(size, HexFormat.of().formatHex(digest.digest()),
                Files.getLastModifiedTime(path, LinkOption.NOFOLLOW_LINKS).toInstant());
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    @Override
    public void close() throws IOException {
        // TWR preserves a close failure and suppresses a later deletion failure, even for Error.
        try (var retirement = new DeleteDraft(path)) {
            output.close();
        }
    }

    record Content(long size, String checksum, Instant modified) {
        FileStat stat(String path, String contentType) {
            return new FileStat(path, size, modified, checksum, contentType, checksum, null);
        }
    }

    private record DeleteDraft(Path path) implements AutoCloseable {
        @Override public void close() throws IOException { Files.deleteIfExists(path); }
    }
}
