package org.pangrova.worker.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.PriorityQueue;
import java.util.UUID;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.storage.StorageType;
import org.pangrova.file.api.directory.FileListing;
import org.pangrova.file.api.directory.FileStat;
import org.pangrova.file.api.object.MultipartUpload;
import org.pangrova.file.api.object.ObjectLocation;
import org.pangrova.file.api.object.ObjectStoreAccessor;
import org.pangrova.file.api.object.UploadedPart;

/**
 * Filesystem-backed object store used for local deployments and tests.
 *
 * <p>The profile bucket names the root directory; the space base key is appended
 * below it. Symbolic links below or at the configured root are rejected in every
 * path component, including space roots and multipart staging. The configured
 * directory and its ancestors must be controlled by the Worker administrator;
 * concurrent filesystem edits by another process are outside this backend's
 * isolation boundary. Entity tags and read
 * preconditions use a streaming SHA-256 of the actual content, including for
 * same-length replacements that preserve filesystem timestamps.</p>
 */
public final class LocalObjectStoreAccessor implements ObjectStoreAccessor {

    private static final int COPY_BUFFER_BYTES = 64 * 1024;
    private static final int MAX_MULTIPART_PARTS = 10_000;

    private final Path root;

    public LocalObjectStoreAccessor(Path root) {
        this.root = root.toAbsolutePath().normalize();
    }

    @Override
    public StorageType storageType() {
        return StorageType.LOCAL;
    }

    @Override
    public FileStat head(ObjectLocation location, String path) {
        Path file = resolveExisting(location, path);
        return stat(location, path, file);
    }

    @Override
    public InputStream openRange(ObjectLocation location, String path, long offset, long length,
            String expectedVersion) throws IOException {
        Path file = resolveExisting(location, path);
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ,
                LinkOption.NOFOLLOW_LINKS);
        try {
            // Validate the opened handle, not a path that an atomic upload can replace.
            if (expectedVersion != null && !expectedVersion.equals(contentVersion(channel))) {
                throw new PangrovaException(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED,
                        "Version precondition failed for '" + path + "'");
            }
            long size = channel.size();
            if (offset < 0 || offset > size || length > size - offset) {
                throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                        "Requested range is outside '" + path + "'");
            }
            channel.position(offset);
            InputStream stream = Channels.newInputStream(channel);
            return new BoundedInputStream(stream, length < 0 ? size - offset : length);
        } catch (IOException | RuntimeException failure) {
            channel.close();
            throw failure;
        }
    }

    @Override
    public FileStat put(ObjectLocation location, String path, InputStream data, long size,
            String contentType) throws IOException {
        Path target = resolveForWrite(location, path);
        Path draft = Files.createTempFile(parentDirectory(target), ".pangrova-upload-", ".part");
        try {
            long written = copy(data, draft, size);
            if (size >= 0 && written != size) {
                throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                        "Uploaded bytes do not match the declared size for '" + path + "'");
            }
            Files.move(draft, target, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException failure) {
            Files.deleteIfExists(draft);
            throw failure;
        }
        return head(location, path);
    }

    @Override
    public MultipartUpload beginMultipart(ObjectLocation location, String path,
            String contentType) {
        // Check the destination before creating a provider session or its staging files.
        resolve(location, path);
        String uploadId = UUID.randomUUID().toString();
        try {
            ensureDirectories(partsDirectory(uploadId));
        } catch (IOException failure) {
            throw storageFailure("Cannot create multipart staging for '" + path + "'", failure);
        }
        return new MultipartUpload(uploadId, location, path);
    }

    @Override
    public UploadedPart uploadPart(MultipartUpload upload, int partNumber, InputStream data,
            long size) throws IOException {
        Path part = checkedPath(partsDirectory(upload.uploadId()).resolve(partName(partNumber)));
        Path draft = Files.createTempFile(parentDirectory(part), ".upload-", ".part");
        MessageDigest digest = sha256();
        try {
            long written = copy(new DigestInputStream(data, digest), draft, size);
            if (size >= 0 && written != size) {
                throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                        "Multipart bytes do not match the declared size");
            }
            Files.move(draft, part, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            return new UploadedPart(partNumber, HexFormat.of().formatHex(digest.digest()), written);
        } catch (IOException | RuntimeException failure) {
            try {
                Files.deleteIfExists(draft);
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    public FileStat completeMultipart(MultipartUpload upload, List<UploadedPart> parts) {
        if (parts.isEmpty()) {
            throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                    "Multipart upload has no parts: " + upload.uploadId());
        }
        Path target = resolveForWrite(upload.location(), upload.path());
        Path uploadDirectory = partsDirectory(upload.uploadId());
        try {
            Path draft = Files.createTempFile(parentDirectory(target), ".pangrova-upload-", ".part");
            List<UploadedPart> ordered = new ArrayList<>(parts);
            ordered.sort(Comparator.comparingInt(UploadedPart::partNumber));
            try {
                try (OutputStream output = Files.newOutputStream(draft,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                    int expectedPart = 1;
                    for (UploadedPart part : ordered) {
                        if (part.partNumber() != expectedPart) {
                            throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                                    "Multipart parts are not contiguous at part " + expectedPart);
                        }
                        Path source = checkedPath(uploadDirectory.resolve(partName(part.partNumber())));
                        if (!Files.exists(source)) {
                            throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                                    "Multipart part " + part.partNumber() + " is missing");
                        }
                        copyVerifiedPart(source, output, part);
                        expectedPart++;
                    }
                }
                Files.move(draft, target, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException | RuntimeException failure) {
                try {
                    Files.deleteIfExists(draft);
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            FileStat completed = head(upload.location(), upload.path());
            try {
                abortMultipart(upload);
            } catch (PangrovaException ignored) {
                // Staged parts are best-effort cleanup after the object is committed.
            }
            return completed;
        } catch (IOException failure) {
            throw storageFailure("Cannot complete multipart upload for '" + upload.path() + "'",
                    failure);
        }
    }

    private static void copyVerifiedPart(Path source, OutputStream output, UploadedPart part)
            throws IOException {
        MessageDigest digest = sha256();
        long copied;
        try (InputStream input = new DigestInputStream(
                Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS), digest)) {
            copied = input.transferTo(output);
        }
        if (copied != part.size()
                || !HexFormat.of().formatHex(digest.digest()).equals(part.etag())) {
            throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                    "Multipart part " + part.partNumber() + " does not match its ETag or size");
        }
    }

    @Override
    public void abortMultipart(MultipartUpload upload) {
        Path directory = partsDirectory(upload.uploadId());
        if (!Files.exists(directory)) {
            return;
        }
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (IOException failure) {
            throw storageFailure("Cannot abort multipart upload " + upload.uploadId(), failure);
        }
    }

    @Override
    public FileListing list(ObjectLocation location, String prefix, String pageToken, int pageSize) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("pageSize must be positive");
        }
        Path base = resolve(location, prefix == null ? "" : prefix);
        if (pageToken != null && !pageToken.isEmpty()) {
            location.key(pageToken);
        }
        // Keep only the next page and one lookahead key, not metadata for the entire tree.
        // Metadata includes a content hash, so compute it only for keys actually returned.
        PriorityQueue<String> candidates = new PriorityQueue<>(Comparator.reverseOrder());
        if (Files.isRegularFile(base, LinkOption.NOFOLLOW_LINKS)) {
            selectCandidate(relativePath(location, base), pageToken, pageSize, candidates);
        } else if (Files.isDirectory(base, LinkOption.NOFOLLOW_LINKS)) {
            collect(location, base, pageToken, pageSize, candidates);
        }
        boolean hasMore = candidates.size() > pageSize;
        List<FileStat> page = candidates.stream().sorted().limit(pageSize)
                .map(path -> head(location, path)).toList();
        String next = hasMore
                ? page.get(page.size() - 1).path() : null;
        return new FileListing(page, next);
    }

    @Override
    public void delete(ObjectLocation location, String path, String expectedVersion) {
        Path file = resolveExisting(location, path);
        requireVersion(stat(location, path, file), expectedVersion, path);
        try {
            Files.deleteIfExists(file);
        } catch (IOException failure) {
            throw storageFailure("Cannot delete '" + path + "'", failure);
        }
    }

    private void collect(ObjectLocation location, Path directory, String pageToken,
            int pageSize, PriorityQueue<String> candidates) {
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
                    if (attributes.isRegularFile()) {
                        String relative = relativePath(location, file);
                        selectCandidate(relative, pageToken, pageSize, candidates);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException failure) {
            throw storageFailure("Cannot list '" + directory + "'", failure);
        }
    }

    private static void selectCandidate(String path, String pageToken, int pageSize,
            PriorityQueue<String> candidates) {
        if (pageToken != null && !pageToken.isEmpty() && path.compareTo(pageToken) <= 0) {
            return;
        }
        candidates.add(path);
        if (candidates.size() > (long) pageSize + 1) {
            candidates.remove();
        }
    }

    private FileStat stat(ObjectLocation location, String path, Path file) {
        try {
            BasicFileAttributes attributes = Files.readAttributes(file,
                    BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isRegularFile()) {
                throw notFound(path);
            }
            Instant modified = attributes.lastModifiedTime().toInstant();
            String version;
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ,
                    LinkOption.NOFOLLOW_LINKS)) {
                version = contentVersion(channel);
            }
            BasicFileAttributes after = Files.readAttributes(file,
                    BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!java.util.Objects.equals(attributes.fileKey(), after.fileKey())
                    || !attributes.lastModifiedTime().equals(after.lastModifiedTime())
                    || attributes.size() != after.size()) {
                throw new PangrovaException(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED,
                        "File changed while reading metadata for '" + path + "'");
            }
            String contentType = Files.probeContentType(file);
            return new FileStat(path, attributes.size(), modified, version, contentType,
                    version, null);
        } catch (IOException failure) {
            throw storageFailure("Cannot stat '" + path + "'", failure);
        }
    }

    private static String contentVersion(FileChannel channel) throws IOException {
        MessageDigest digest = sha256();
        ByteBuffer buffer = ByteBuffer.allocate(COPY_BUFFER_BYTES);
        channel.position(0);
        while (channel.read(buffer) != -1) {
            buffer.flip();
            digest.update(buffer);
            buffer.clear();
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("Required SHA-256 algorithm is unavailable", failure);
        }
    }

    private String relativePath(ObjectLocation location, Path file) {
        Path base = root.resolve(location.baseKey()).normalize();
        return base.relativize(file).toString().replace('\\', '/');
    }

    private Path partsDirectory(String uploadId) {
        // A provider handle must never become a filesystem path supplied by the caller.
        if (uploadId == null || !uploadId.matches(
                "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")) {
            throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                    "Invalid local multipart upload identifier");
        }
        return checkedPath(root.resolve(".pangrova-multipart").resolve(uploadId));
    }

    private static String partName(int partNumber) {
        if (partNumber < 1 || partNumber > MAX_MULTIPART_PARTS) {
            throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                    "Multipart part number must be between 1 and 10000");
        }
        return String.format("%05d.part", partNumber);
    }

    private Path resolve(ObjectLocation location, String path) {
        String key = location.key(path);
        Path candidate = root.resolve(key).normalize();
        if (!candidate.startsWith(root)) {
            throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                    "Path escapes the local storage root: " + path);
        }
        return checkedPath(candidate);
    }

    /** Check all existing components before reading, listing, deleting, or creating anything. */
    private Path checkedPath(Path candidate) {
        if (!candidate.normalize().startsWith(root)) {
            throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                    "Path escapes the local storage root");
        }
        Path current = root;
        try {
            checkNotSymbolicLink(current);
            for (Path component : root.relativize(candidate)) {
                current = current.resolve(component);
                checkNotSymbolicLink(current);
            }
        } catch (NoSuchFileException missing) {
            // Descendants of a missing component cannot exist; writes create them below.
        } catch (IOException failure) {
            throw storageFailure("Cannot validate local storage path", failure);
        }
        return candidate;
    }

    private static void checkNotSymbolicLink(Path path) throws IOException {
        if (Files.readAttributes(path, BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS).isSymbolicLink()) {
            throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                    "Symbolic links are not allowed in local file-space paths");
        }
    }

    private void ensureDirectories(Path directory) throws IOException {
        checkedPath(directory);
        Files.createDirectories(root);
        checkNotSymbolicLink(root);
        Path current = root;
        for (Path component : root.relativize(directory)) {
            current = current.resolve(component);
            try {
                Files.createDirectory(current);
            } catch (java.nio.file.FileAlreadyExistsException existing) {
                checkNotSymbolicLink(current);
                if (!Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                    throw existing;
                }
            }
        }
    }

    private Path resolveExisting(ObjectLocation location, String path) {
        Path candidate = resolve(location, path);
        if (!Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS)) {
            throw notFound(path);
        }
        return candidate;
    }

    private static Path parentDirectory(Path path) {
        Path parent = path.getParent();
        if (parent == null) {
            throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                    "File destination must have a parent directory");
        }
        return parent;
    }

    private Path resolveForWrite(ObjectLocation location, String path) {
        Path candidate = resolve(location, path);
        try {
            ensureDirectories(parentDirectory(candidate));
        } catch (IOException failure) {
            throw storageFailure("Cannot prepare '" + path + "'", failure);
        }
        return candidate;
    }

    private static void requireVersion(FileStat stat, String expectedVersion, String path) {
        if (expectedVersion != null && !expectedVersion.equals(stat.providerVersion())) {
            throw new PangrovaException(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED,
                    "Version precondition failed for '" + path + "'");
        }
    }

    private static long copy(InputStream data, Path target, long expectedSize) throws IOException {
        long written = 0;
        byte[] buffer = new byte[COPY_BUFFER_BYTES];
        try (OutputStream output = Files.newOutputStream(target,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            int read;
            while ((read = data.read(buffer)) != -1) {
                written += read;
                if (expectedSize >= 0 && written > expectedSize) {
                    throw new PangrovaException(PangrovaErrorCode.FILE_LIMIT_EXCEEDED,
                            "Upload exceeds the declared size");
                }
                output.write(buffer, 0, read);
            }
        }
        return written;
    }

    private static PangrovaException notFound(String path) {
        return new PangrovaException(PangrovaErrorCode.FILE_OBJECT_NOT_FOUND,
                "File object not found: " + path);
    }

    private static PangrovaException storageFailure(String message, Exception cause) {
        return new PangrovaException(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE, message, cause);
    }

    /** Caps a stream at a known length without trusting the underlying channel. */
    private static final class BoundedInputStream extends InputStream {

        private final InputStream delegate;
        private long remaining;

        BoundedInputStream(InputStream delegate, long limit) {
            this.delegate = delegate;
            this.remaining = limit;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int value = delegate.read();
            if (value >= 0) {
                remaining--;
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int bounded = (int) Math.min(length, remaining);
            int read = delegate.read(buffer, offset, bounded);
            if (read > 0) {
                remaining -= read;
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
