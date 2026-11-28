package org.pangrova.connector.paimon.write;

import org.pangrova.common.crypto.Sha256Digests;

import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;

/** A local path is meaningful only in the original kernel and at the same opened parent inode. */
final class PaimonLocalStagingAuthority {
    private static final String BOOT_ID = readBootId();

    private PaimonLocalStagingAuthority() { }

    static Path absolutePath(URI uri) {
        Path path;
        if ("file".equals(uri.getScheme()) && uri.getAuthority() == null) {
            path = Path.of(uri);
        } else if (uri.getScheme() == null && uri.getAuthority() == null && uri.getPath() != null) {
            path = Path.of(uri.getPath());
        } else {
            return null;
        }
        return path.isAbsolute() ? path : null;
    }

    static String observe(Path parent) throws IOException {
        if (BOOT_ID.isEmpty() || parent == null) return "";
        try {
            return fingerprint(Files.readAttributes(parent, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS));
        } catch (NoSuchFileException | AccessDeniedException unavailable) {
            return "";
        }
    }

    static String observe(SecureDirectoryStream<Path> directory) throws IOException {
        if (BOOT_ID.isEmpty()) return "";
        var view = directory.getFileAttributeView(BasicFileAttributeView.class);
        return view == null ? "" : fingerprint(view.readAttributes());
    }

    private static String fingerprint(BasicFileAttributes attributes) {
        Object key = attributes.fileKey();
        if (!attributes.isDirectory() || key == null) return "";
        return Sha256Digests.sha256Hex(BOOT_ID + ":" + key);
    }

    private static String readBootId() {
        try {
            return Files.readString(Path.of("/proc/sys/kernel/random/boot_id")).strip();
        } catch (IOException unavailable) {
            // Other kernels cannot attest this local namespace with the Linux boot identity.
            return "";
        }
    }
}
