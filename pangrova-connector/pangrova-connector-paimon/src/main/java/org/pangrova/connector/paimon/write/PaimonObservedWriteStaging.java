package org.pangrova.connector.paimon.write;

import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.write.PaimonSdkWriteFragment.StagedObject;

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
        if (fragments.isEmpty()) return false;
        Path root = PaimonLocalStagingAuthority.absolutePath(
                new org.apache.paimon.fs.Path(handle.tableLocation()).toUri());
        if (root == null || !root.equals(root.normalize())) return false;
        Path parentPath = root.getParent();
        if (parentPath == null) return false;
        try (var parent = Files.newDirectoryStream(parentPath)) {
            if (!(parent instanceof SecureDirectoryStream<Path> secureParent)) return false;
            String authority = PaimonLocalStagingAuthority.observe(secureParent);
            if (authority.isEmpty()) return false;
            Map<Path, StagedObject> objects = collect(handle, fragments, root, authority);
            if (objects == null || objects.isEmpty()) return false;
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

    private static Map<Path, StagedObject> collect(PaimonOutputTableHandle handle, List<byte[]> fragments, Path root,
            String authority) throws IOException {
        Map<Path, StagedObject> objects = new LinkedHashMap<>();
        for (byte[] bytes : fragments) {
            var fragment = PaimonSdkWriteFragment.decode(bytes, handle);
            if (!fragment.objects().isEmpty() && !authority.equals(fragment.localAuthority())) return null;
            for (var object : fragment.objects()) {
                if (object.localFingerprint().isEmpty()) return null;
                collectObject(objects, root, object);
            }
        }
        return objects;
    }

    private static void collectObject(Map<Path, StagedObject> objects, Path root, StagedObject object) {
        Path path = PaimonLocalStagingAuthority.absolutePath(URI.create(object.uri()));
        if (path == null || !path.equals(path.normalize()) || !path.startsWith(root) || root.equals(path)) {
            throw new IllegalArgumentException("Observed Paimon object is outside its bound table root");
        }
        StagedObject earlier = objects.putIfAbsent(root.relativize(path), object);
        if (earlier != null && !earlier.equals(object)) {
            throw new IllegalArgumentException("Conflicting receipts for observed Paimon object");
        }
    }

    private static void reclaim(SecureDirectoryStream<Path> directory, Path relative, int depth, StagedObject object)
            throws IOException {
        Path name = relative.getName(depth);
        try {
            if (depth + 1 < relative.getNameCount()) {
                try (var child = directory.newDirectoryStream(name, LinkOption.NOFOLLOW_LINKS)) {
                    reclaim(child, relative, depth + 1, object);
                }
                return;
            }
            var attributes = directory.getFileAttributeView(
                    name, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).readAttributes();
            if (!object.localFingerprint().equals(
                    PaimonLocalStagingAuthority.objectFingerprint(attributes, object.sizeBytes()))) {
                throw new IOException("Observed Paimon staged file differs from its closed-file receipt: " + relative);
            }
            directory.deleteFile(name);
        } catch (NoSuchFileException alreadyReclaimed) {
            // Missing is idempotent for this exact allocation and still proves absence.
        }
    }
}
