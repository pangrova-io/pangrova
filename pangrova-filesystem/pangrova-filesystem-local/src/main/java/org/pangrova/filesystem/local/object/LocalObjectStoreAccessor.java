package org.pangrova.filesystem.local.object;

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
import java.time.Instant;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.PriorityQueue;
import java.util.UUID;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.enums.StorageType;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.filesystem.api.object.MultipartUpload;
import org.pangrova.filesystem.api.object.ObjectLocation;
import org.pangrova.filesystem.api.object.ObjectStoreAccessor;
import org.pangrova.filesystem.api.object.UploadedPart;

/**
 * Filesystem-backed object store used for local deployments and tests.
 *
 * <p>The profile bucket names the root directory; the space base key is appended
 * below it. Symbolic links below or at the configured root are rejected in every
 * path component, including space roots and multipart staging. The configured
 * directory and its ancestors must be controlled by the Worker administrator;
 * participating accessors coordinate mutations through canonical directory OS locks.
 * Uncoordinated edits by other programs remain outside this boundary. Entity tags and read
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
            if (offset < 0 || offset > size || length < -1 || length > size - offset) {
                throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                        "Requested range is outside '" + path + "'");
            }
            channel.position(offset);
            InputStream stream = Channels.newInputStream(channel);
            return new BoundedInputStream(stream, length < 0 ? size - offset : length);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                channel.close();
            } catch (IOException | RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    public FileStat put(ObjectLocation location, String path, InputStream data, long size,
            String contentType) throws IOException {
        Path target = resolveForWrite(location, path);
        boolean publicationAttempted = false;
        try (var draft = LocalObjectDraft.create(parentDirectory(target))) {
            draft.copy(data, size);
            var content = draft.seal();
            String detectedType = Files.probeContentType(target);
            try (var lease = LocalMutationLocks.acquire(mutationLock(target))) {
                checkedPath(target);
                publicationAttempted = true;
                Files.move(draft.path(), target, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
                lease.forceDirectories();
            }
            return content.stat(path, detectedType);
        } catch (IOException | RuntimeException failure) {
            if (!publicationAttempted) throw failure;
            String message = "Local object publication was attempted; its outcome requires reconciliation";
            if (failure instanceof IOException io) throw new IOException(message, io);
            throw storageFailure(message, failure);
        }
    }

    @Override
    public MultipartUpload beginMultipart(ObjectLocation location, String path,
            String contentType) {
        // Check the destination before creating a provider session or its staging files.
        resolve(location, path);
        String uploadId = UUID.randomUUID().toString();
        Path directory = partsDirectory(uploadId);
        boolean created = false;
        try {
            ensureDirectories(parentDirectory(directory));
            Files.createDirectory(directory);
            created = true;
            try (var output = new java.io.DataOutputStream(Files.newOutputStream(
                    checkedPath(directory.resolve(".pangrova-active")), StandardOpenOption.CREATE_NEW))) {
                output.writeUTF(location.key(path));
            }
            try (var binding = FileChannel.open(directory.resolve(".pangrova-active"), StandardOpenOption.WRITE)) {
                binding.force(true);
            }
            LocalMutationLocks.forceDirectory(directory);
            LocalMutationLocks.forceDirectory(parentDirectory(directory));
        } catch (IOException | RuntimeException | Error failure) {
            try {
                if (created) deleteFailedSession(directory);
            } catch (IOException | RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            if (failure instanceof IOException io) {
                throw storageFailure("Cannot create multipart staging for '" + path + "'", io);
            }
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw (Error) failure;
        }
        return new MultipartUpload(uploadId, location, path);
    }

    @Override
    public UploadedPart uploadPart(MultipartUpload upload, int partNumber, InputStream data,
            long size) throws IOException {
        Path directory = partsDirectory(upload.uploadId());
        Path part = checkedPath(directory.resolve(partName(partNumber)));
        // A draft lives beside the session, so abort can retire the session while an input is blocked.
        try (var draft = LocalObjectDraft.create(parentDirectory(directory))) {
            try (var lease = lockSession(directory)) {
                requireActive(upload, directory);
            }
            draft.copy(data, size);
            var content = draft.seal();
            try (var lease = lockSession(directory)) {
                requireActive(upload, directory);
                checkedPath(part);
                Files.move(draft.path(), part, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
                LocalMutationLocks.forceDirectory(directory);
                lease.forceDirectories();
            }
            return new UploadedPart(partNumber, content.checksum(), content.size());
        }
    }

    @Override
    public FileStat completeMultipart(MultipartUpload upload, List<UploadedPart> parts) {
        if (parts.isEmpty()) {
            throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                    "Multipart upload has no parts: " + upload.uploadId());
        }
        Path target = resolveForWrite(upload.location(), upload.path());
        Path directory = partsDirectory(upload.uploadId());
        boolean completionAttempted = false;
        boolean publicationAttempted = false;
        try (var draft = LocalObjectDraft.create(parentDirectory(target))) {
            int expected = 1;
            for (UploadedPart part : parts.stream()
                    .sorted(Comparator.comparingInt(UploadedPart::partNumber)).toList()) {
                if (part.partNumber() != expected++) {
                    throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                            "Multipart parts are not contiguous");
                }
                copyPart(upload, directory, draft.output(), part);
            }
            var content = draft.seal();
            String contentType = Files.probeContentType(target);
            try (var lease = LocalMutationLocks.acquire(mutationLock(target), sessionLock(directory))) {
                requireActive(upload, directory);
                checkedPath(target);
                // Consume before publication: a lost acknowledgement must not make a retry overwrite a later writer.
                completionAttempted = true;
                Files.move(checkedPath(directory.resolve(".pangrova-active")),
                        checkedPath(directory.resolve(".pangrova-completing")), StandardCopyOption.ATOMIC_MOVE);
                LocalMutationLocks.forceDirectory(directory);
                publicationAttempted = true;
                Files.move(draft.path(), target, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
                lease.forceDirectories();
            }
            try {
                abortMultipart(upload);
            } catch (RuntimeException cleanup) {
                throw storageFailure("Local multipart committed but staging cleanup failed", cleanup);
            }
            return content.stat(upload.path(), contentType);
        } catch (IOException failure) {
            String message = publicationAttempted
                    ? "Local multipart publication was attempted; its outcome requires reconciliation"
                    : completionAttempted
                            ? "Local upload completion admission was attempted; inspect or abort the session"
                    : "Cannot prepare local multipart publication";
            throw storageFailure(message, failure);
        }
    }

    private void copyPart(MultipartUpload upload, Path directory, OutputStream output, UploadedPart part)
            throws IOException {
        InputStream input;
        try (var lease = lockSession(directory)) {
            requireActive(upload, directory);
            Path source = checkedPath(directory.resolve(partName(part.partNumber())));
            if (!Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
                throw new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID,
                        "Multipart part " + part.partNumber() + " is missing");
            }
            input = Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS);
        }
        copyVerifiedPart(input, output, part);
    }

    private static void copyVerifiedPart(InputStream source, OutputStream output, UploadedPart part)
            throws IOException {
        MessageDigest digest = DigestUtils.sha256();
        long copied;
        try (InputStream input = new DigestInputStream(source, digest)) {
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
        if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) return;
        try (var lease = lockSession(directory)) {
            if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) return;
            Path active = checkedPath(directory.resolve(".pangrova-active"));
            Path completing = checkedPath(directory.resolve(".pangrova-completing"));
            if (Files.exists(active, LinkOption.NOFOLLOW_LINKS)) {
                requireBinding(upload, active);
                Files.move(active, completing, StandardCopyOption.ATOMIC_MOVE);
                LocalMutationLocks.forceDirectory(directory);
            } else if (Files.exists(completing, LinkOption.NOFOLLOW_LINKS)) {
                requireBinding(upload, completing);
            } else {
                throw invalidUpload("Local multipart binding is missing");
            }
            deleteFailedSession(directory);
            lease.forceDirectories();
        } catch (IOException failure) {
            throw storageFailure("Cannot abort multipart upload " + upload.uploadId(), failure);
        }
    }

    private static void deleteFailedSession(Path directory) throws IOException {
        if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) return;
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
        }
    }

    private void requireActive(MultipartUpload upload, Path directory) throws IOException {
        if (Files.exists(checkedPath(directory.resolve(".pangrova-completing")), LinkOption.NOFOLLOW_LINKS)) {
            throw invalidUpload("Local upload was already consumed");
        }
        Path active = checkedPath(directory.resolve(".pangrova-active"));
        if (!Files.isRegularFile(active, LinkOption.NOFOLLOW_LINKS)) throw invalidUpload("Local upload is not active");
        requireBinding(upload, active);
    }

    private static void requireBinding(MultipartUpload upload, Path binding) throws IOException {
        try (var input = new java.io.DataInputStream(Files.newInputStream(binding, LinkOption.NOFOLLOW_LINKS))) {
            if (!input.readUTF().equals(upload.location().key(upload.path())) || input.read() != -1) {
                throw invalidUpload("Multipart upload does not belong to this object");
            }
        }
    }

    private static PangrovaException invalidUpload(String message) {
        return new PangrovaException(PangrovaErrorCode.FILE_UPLOAD_STATE_INVALID, message);
    }

    private LocalMutationLocks lockSession(Path directory) throws IOException {
        try {
            return LocalMutationLocks.acquire(sessionLock(directory));
        } catch (NoSuchFileException missing) {
            throw invalidUpload("Local upload is not active");
        }
    }

    private Path mutationLock(Path target) {
        return checkedPath(parentDirectory(target).resolve(LocalMutationLocks.FILE_NAME));
    }

    private Path sessionLock(Path directory) {
        return checkedPath(parentDirectory(directory).resolve(LocalMutationLocks.FILE_NAME));
    }

    @Override
    public FileListing list(ObjectLocation location, String prefix, String pageToken, int pageSize) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("pageSize must be positive");
        }
        Path base = resolve(location, prefix == null ? "" : prefix);
        if (pageToken != null && !pageToken.isEmpty()) {
            resolve(location, pageToken);
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
        try (var lease = LocalMutationLocks.acquire(mutationLock(file))) {
            file = resolveExisting(location, path);
            requireVersion(stat(location, path, file), expectedVersion, path);
            Files.deleteIfExists(file);
            lease.forceDirectories();
        } catch (IOException failure) {
            throw storageFailure("Cannot delete '" + path + "'", failure);
        }
    }

    private void collect(ObjectLocation location, Path directory, String pageToken,
            int pageSize, PriorityQueue<String> candidates) {
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes) {
                    Path name = directory.getFileName();
                    return name != null && reserved(name.toString())
                            ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
                    Path name = file.getFileName();
                    if (attributes.isRegularFile() && name != null && !reserved(name.toString())) {
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
        MessageDigest digest = DigestUtils.sha256();
        ByteBuffer buffer = ByteBuffer.allocate(COPY_BUFFER_BYTES);
        channel.position(0);
        while (channel.read(buffer) != -1) {
            buffer.flip();
            digest.update(buffer);
            buffer.clear();
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static boolean reserved(String name) {
        return name.equals(".pangrova-multipart") || name.equals(LocalMutationLocks.FILE_NAME)
                || name.startsWith(".pangrova-upload-");
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
        return String.format(java.util.Locale.ROOT, "%05d.part", partNumber);
    }

    private Path resolve(ObjectLocation location, String path) {
        String key = location.key(path);
        for (String component : key.split("/")) {
            if (reserved(component)) {
                throw new PangrovaException(PangrovaErrorCode.FILE_PATH_INVALID,
                        "Path addresses private local storage state");
            }
        }
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
        var missing = new java.util.ArrayList<Path>();
        for (Path entry = root; entry != null && !Files.exists(entry, LinkOption.NOFOLLOW_LINKS);
                entry = entry.getParent()) {
            missing.add(entry);
        }
        Files.createDirectories(root);
        checkNotSymbolicLink(root);
        for (Path entry : missing) {
            LocalMutationLocks.forceDirectory(entry);
            Path parent = entry.getParent();
            if (parent != null) LocalMutationLocks.forceDirectory(parent);
        }
        Path current = root;
        for (Path component : root.relativize(directory)) {
            current = current.resolve(component);
            try {
                Files.createDirectory(current);
                LocalMutationLocks.forceDirectory(current);
                LocalMutationLocks.forceDirectory(parentDirectory(current));
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
            if (value < 0) throw new java.io.EOFException("Local object ended before its declared range");
            remaining--;
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            java.util.Objects.checkFromIndexSize(offset, length, buffer.length);
            if (length == 0) return 0;
            if (remaining <= 0) {
                return -1;
            }
            int bounded = (int) Math.min(length, remaining);
            int read = delegate.read(buffer, offset, bounded);
            if (read < 0) throw new java.io.EOFException("Local object ended before its declared range");
            remaining -= read;
            return read;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
