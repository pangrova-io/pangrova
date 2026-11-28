package org.pangrova.connector.paimon.write;

import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reclaims independently allocated local filenames using directory-relative operations. */
public final class PaimonObservedWriteStaging {
    private PaimonObservedWriteStaging() { }

    /** Deletes the observed files when the local storage authority matches. */
    public static boolean reclaim(PaimonOutputTableHandle handle, List<byte[]> fragments) throws IOException {
        Path root = PaimonLocalStagingAuthority.absolutePath(
                new org.apache.paimon.fs.Path(handle.tableLocation()).toUri());
        if (root == null || !root.equals(root.normalize())) return false;
        Path parentPath = root.getParent();
        if (parentPath == null) return false;
        try (var parent = Files.newDirectoryStream(parentPath)) {
            if (!(parent instanceof SecureDirectoryStream<Path> secureParent)) return false;
            String authority = PaimonLocalStagingAuthority.observe(secureParent);
            if (authority.isEmpty()) return false;
            Map<Path, Long> objects = collect(handle, fragments, root, authority);
            if (objects == null) return false;
            try (var directory = secureParent.newDirectoryStream(root.getFileName(), LinkOption.NOFOLLOW_LINKS)) {
                for (var object : objects.entrySet()) {
                    reclaim(directory, object.getKey(), 0, object.getValue());
                }
            } catch (NoSuchFileException absentRoot) {
                // The proved parent remains, so absence of the table root settles its observed objects.
            }
        } catch (NoSuchFileException absentAuthority) {
            return false;
        }
        return true;
    }

    private static Map<Path, Long> collect(PaimonOutputTableHandle handle, List<byte[]> fragments, Path root,
            String authority) throws IOException {
        Map<Path, Long> objects = new LinkedHashMap<>();
        for (byte[] bytes : fragments) {
            var fragment = PaimonSdkWriteFragment.decode(bytes, handle);
            if (!fragment.objects().isEmpty() && !authority.equals(fragment.localAuthority())) return null;
            for (var object : fragment.objects()) {
                collectObject(objects, root, URI.create(object.uri()), object.sizeBytes());
            }
        }
        return objects;
    }

    private static void collectObject(Map<Path, Long> objects, Path root, URI uri, long size) {
        Path path = PaimonLocalStagingAuthority.absolutePath(uri);
        if (path == null || !path.equals(path.normalize()) || !path.startsWith(root) || root.equals(path)) {
            throw new IllegalArgumentException("Observed Paimon object is outside its bound table root");
        }
        Long earlier = objects.putIfAbsent(root.relativize(path), size);
        if (earlier != null && earlier != size) {
            throw new IllegalArgumentException("Conflicting lengths for observed Paimon object");
        }
    }

    private static void reclaim(SecureDirectoryStream<Path> directory, Path relative, int depth, long size)
            throws IOException {
        Path name = relative.getName(depth);
        try {
            if (depth + 1 < relative.getNameCount()) {
                try (var child = directory.newDirectoryStream(name, LinkOption.NOFOLLOW_LINKS)) {
                    reclaim(child, relative, depth + 1, size);
                }
                return;
            }
            var attributes = directory.getFileAttributeView(
                    name, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
            if (!attributes.isRegularFile() || attributes.size() != size) {
                throw new IOException("Observed Paimon staged file differs from its closed-file receipt: " + relative);
            }
            directory.deleteFile(name);
        } catch (NoSuchFileException alreadyReclaimed) {
            // Missing is idempotent for this exact allocation and still proves absence.
        }
    }
}
