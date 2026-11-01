package org.pangrova.shuffle.storage.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.pangrova.shuffle.api.storage.ShuffleObjectStorage;

/**
 * Local-filesystem immutable batch storage using atomic file publication.
 *
 * <p>Objects live under {@code &lt;baseDir&gt;/&lt;prefix&gt;/&lt;key&gt;}. Publication is
 * crash-safe: content is written and checksum-verified in a sibling temp file and only then moved
 * into place, so a crashed writer can never leave a partial object visible. Content-identical
 * re-publication is idempotent; conflicting content fails fast.
 */
public final class LocalFileShuffleObjectStorage implements ShuffleObjectStorage {

    private final Path baseDir;
    private final String prefix;

    public LocalFileShuffleObjectStorage(Path baseDir, String prefix) {
        if (baseDir == null) {
            throw new IllegalArgumentException("baseDir must not be null");
        }
        validateKey(prefix, "prefix");
        this.baseDir = baseDir;
        this.prefix = prefix;
    }

    @Override
    public void putImmutable(String key, Supplier<InputStream> data, long sizeBytes, byte[] sha256) {
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("sizeBytes must not be negative: " + sizeBytes);
        }
        validateKey(key, "key");
        Path target = resolve(key);
        try {
            if (Files.exists(target)) {
                reconcileExisting(target, key, sizeBytes, sha256);
                return;
            }
            Path parent = target.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path temp = Files.createTempFile(parent, ".pangrova-", ".tmp");
            try {
                byte[] written;
                try (InputStream in = new DigestInputStream(data.get(), sha256Digest())) {
                    Files.copy(in, temp, StandardCopyOption.REPLACE_EXISTING);
                    written = ((DigestInputStream) in).getMessageDigest().digest();
                }
                if (Files.size(temp) != sizeBytes || !MessageDigest.isEqual(sha256, written)) {
                    throw new IllegalStateException(
                            "shuffle object content does not match declared batch: " + key);
                }
                Files.move(temp, target);
            } catch (java.nio.file.FileAlreadyExistsException e) {
                reconcileExisting(target, key, sizeBytes, sha256);
            } finally {
                Files.deleteIfExists(temp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to store shuffle object " + key, e);
        }
    }

    private static MessageDigest sha256Digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static void reconcileExisting(Path target, String key, long sizeBytes, byte[] sha256)
            throws IOException {
        byte[] existing = Files.readAllBytes(target);
        if (existing.length == sizeBytes
                && MessageDigest.isEqual(sha256, Sha256.digest(existing))) {
            return;
        }
        throw new IllegalStateException(
                "immutable shuffle object conflicts with existing content: " + key);
    }

    @Override
    public void verifyStored(String key, long sizeBytes, byte[] sha256) {
        validateKey(key, "key");
        Path target = resolve(key);
        if (!Files.exists(target)) {
            throw new IllegalStateException("durable shuffle object is missing: " + key);
        }
        try {
            if (Files.size(target) != sizeBytes) {
                throw new IllegalStateException(
                        "durable shuffle object metadata does not match accepted batch: " + key);
            }
            try (InputStream in = Files.newInputStream(target)) {
                if (!MessageDigest.isEqual(sha256, Sha256.digest(in))) {
                    throw new IllegalStateException(
                            "durable shuffle object metadata does not match accepted batch: " + key);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to verify shuffle object " + key, e);
        }
    }

    @Override
    public byte[] get(String key) {
        validateKey(key, "key");
        try {
            return Files.readAllBytes(resolve(key));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read shuffle object " + key, e);
        }
    }

    @Override
    public List<String> listPrefix(String keyPrefix) {
        validateKey(keyPrefix, "keyPrefix");
        Path dir = resolve(keyPrefix);
        try {
            if (!Files.exists(dir) || !Files.isDirectory(dir)) {
                return List.of();
            }
            try (Stream<Path> walk = Files.walk(dir)) {
                return walk.filter(Files::isRegularFile)
                        .map(this::stripPrefix)
                        .sorted(Comparator.naturalOrder())
                        .toList();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("shuffle object listing failed for " + keyPrefix, e);
        }
    }

    @Override
    public void delete(List<String> keys) {
        for (String key : keys) {
            validateKey(key, "key");
            try {
                Files.deleteIfExists(resolve(key));
            } catch (IOException e) {
                throw new UncheckedIOException("shuffle object cleanup failed for " + key, e);
            }
        }
    }

    @Override
    public String deletePrefixPage(String keyPrefix, String after, int limit) {
        if (limit <= 0 || limit > 1000) {
            throw new IllegalArgumentException("object cleanup page limit is invalid");
        }
        List<String> keys = listPrefix(keyPrefix).stream()
                .filter(key -> after == null || key.compareTo(after) > 0)
                .toList();
        List<String> page = new ArrayList<>(keys.subList(0, Math.min(limit, keys.size())));
        delete(page);
        return keys.size() > limit && !page.isEmpty() ? page.get(page.size() - 1) : null;
    }

    @Override
    public void close() {
        // No long-lived resources to release.
    }

    private Path resolve(String key) {
        return baseDir.resolve(prefix).resolve(key);
    }

    private String stripPrefix(Path path) {
        return baseDir.resolve(prefix).relativize(path).toString();
    }

    private static void validateKey(String key, String field) {
        if (key == null
                || key.isBlank()
                || key.startsWith("/")
                || key.contains("\\")
                || key.contains("..")) {
            throw new IllegalArgumentException("Invalid " + field + ": " + key);
        }
    }
}
