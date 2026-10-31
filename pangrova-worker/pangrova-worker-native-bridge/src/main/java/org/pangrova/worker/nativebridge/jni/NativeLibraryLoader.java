package org.pangrova.worker.nativebridge.jni;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.util.EnumSet;
import org.pangrova.common.util.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads the {@code pangrova_native} shared library from the classpath.
 *
 * <p>Single loading strategy — no fallback chains:
 * <ol>
 *   <li>Locate {@code META-INF/native/{os}/{arch}/libpangrova_native.{so|dylib}}
 *       on the classpath (from native-bundle JAR or maven-resources-plugin copy)</li>
 *   <li>Extract to a temp file, set execute permission, {@link System#load(String)}</li>
 * </ol>
 *
 * <p>This is the same path for development, CI, and production. The dylib
 * reaches the classpath via:
 * <ul>
 *   <li>Local dev: {@code package-native-lib.sh} stages the canonical resource,
 *       then Maven copies it to {@code target/classes/META-INF/native/...}</li>
 *   <li>CI/production: pangrova-native-bundle classifier JAR on the classpath</li>
 * </ul>
 *
 * <p>Thread safety: double-checked locking with volatile flag. Idempotent.</p>
 */
public final class NativeLibraryLoader {

    private static final Logger LOG =
            LoggerFactory.getLogger(NativeLibraryLoader.class);

    private static final String LIBRARY_NAME = "pangrova_native";
    private static final String RESOURCE_PREFIX = "META-INF/native/";

    private static volatile boolean loaded;

    private NativeLibraryLoader() {}

    /**
     * Loads the native library from the classpath. Idempotent and thread-safe.
     *
     * @throws UnsatisfiedLinkError if the library resource is missing or cannot be loaded
     */
    public static void load() {
        if (loaded) {
            return;
        }
        synchronized (NativeLibraryLoader.class) {
            if (loaded) {
                return;
            }
            doLoad();
            loaded = true;
        }
    }

    /**
     * Returns true if the native library is available on the classpath.
     * Does NOT load the library — call {@link #load()} for that.
     * Readiness checks use this probe to fail with an actionable packaging error.
     */
    public static boolean isAvailable() {
        if (loaded) {
            return true;
        }
        String os = detectOs();
        String arch = detectArch();
        String fileName = mapLibraryFileName(os, LIBRARY_NAME);
        String resourcePath = RESOURCE_PREFIX + os + "/" + arch + "/" + fileName;
        return NativeLibraryLoader.class.getClassLoader().getResource(resourcePath) != null;
    }

    private static volatile Boolean functionalCache;

    /**
     * Returns true if the native library is available and can be loaded.
     * Attempts a minimal {@link System#load}
     * via the regular loader; if the dylib resource is missing, or extraction
     * fails, or any cached probe stored earlier failed, returns false.
     *
     * <p>This probe does not verify the {@code nativeXxx} symbols Java declares.
     * For JNI ABI verification, callers must attempt
     * the real init and call {@link #markUnsupported()} on UnsatisfiedLinkError
     * to cache a {@code false} for subsequent test classes.
     *
     * <p>Used by E2E readiness checks, where a false result is a test failure.
     */
    public static boolean isFunctional() {
        if (functionalCache != null) {
            return functionalCache;
        }
        synchronized (NativeLibraryLoader.class) {
            if (functionalCache != null) {
                return functionalCache;
            }
            if (!isAvailable()) {
                functionalCache = false;
                return false;
            }
            try {
                load();
                functionalCache = true;
            } catch (UnsatisfiedLinkError | RuntimeException e) {
                functionalCache = false;
            }
            return functionalCache;
        }
    }

    /**
     * Marks the native library as unsupported (ABI mismatch). Subsequent
     * {@link #isFunctional()} calls return false. Used after catching an
     * {@code UnsatisfiedLinkError} from a runtime init attempt so that
     * later readiness checks fail deterministically without re-attempting init.
     */
    public static void markUnsupported() {
        synchronized (NativeLibraryLoader.class) {
            functionalCache = false;
        }
    }

    private static void doLoad() {
        String os = detectOs();
        String arch = detectArch();
        String fileName = mapLibraryFileName(os, LIBRARY_NAME);
        String resourcePath = RESOURCE_PREFIX + os + "/" + arch + "/" + fileName;

        URL resource = NativeLibraryLoader.class.getClassLoader().getResource(resourcePath);
        if (resource == null) {
            throw new UnsatisfiedLinkError(
                    "Native library not found on classpath: " + resourcePath
                    + ". For local development, run pangrova-worker-native/bin/package-native-lib.sh"
                    + " before Maven copies the packaged resource."
                    + " For production, ensure pangrova-native-bundle JAR is on the classpath.");
        }

        try {
            Path tempFile = Files.createTempFile("pangrova-native-", "-" + fileName);
            // Schedule cleanup immediately — ensures temp file is removed even if
            // System.load() throws UnsatisfiedLinkError (ABI mismatch, wrong arch).
            tempFile.toFile().deleteOnExit();
            extractVerifiedResource(NativeLibraryLoader.class.getClassLoader(), resourcePath, resource, tempFile);
            setExecutePermission(tempFile);
            System.load(tempFile.toAbsolutePath().toString());
            LOG.info("event=native_library_loaded source=classpath os={} arch={}", os, arch);
        } catch (IOException e) {
            throw new UnsatisfiedLinkError(
                    "Failed to extract native library from classpath: " + e.getMessage());
        }
    }

    /** Validates every same-name classpath artifact against the bytes actually extracted for loading. */
    static void extractVerifiedResource(ClassLoader loader, String resourcePath, URL source, Path target)
            throws IOException {
        byte[] expected = extract(source, target);
        var resources = loader.getResources(resourcePath);
        while (resources.hasMoreElements()) {
            URL candidate = resources.nextElement();
            // Resource identity is lexical; URL.equals may perform blocking DNS resolution.
            if (candidate.toExternalForm().equals(source.toExternalForm())) {
                continue;
            }
            MessageDigest digest = DigestUtils.sha256();
            try (InputStream input = candidate.openStream()) {
                byte[] buffer = new byte[8192];
                int count;
                while ((count = input.read(buffer)) != -1) {
                    digest.update(buffer, 0, count);
                }
            }
            if (!MessageDigest.isEqual(expected, digest.digest())) {
                throw new UnsatisfiedLinkError("Conflicting native libraries on classpath for "
                        + resourcePath + ": " + source + " and " + candidate);
            }
        }
    }

    private static byte[] extract(URL source, Path target) throws IOException {
        MessageDigest digest = DigestUtils.sha256();
        try (InputStream in = source.openStream();
             OutputStream out = Files.newOutputStream(target)) {
            byte[] buffer = new byte[8192];
            int count;
            while ((count = in.read(buffer)) != -1) {
                out.write(buffer, 0, count);
                digest.update(buffer, 0, count);
            }
        }
        return digest.digest();
    }

    private static void setExecutePermission(Path path) {
        try {
            Files.setPosixFilePermissions(path, EnumSet.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE));
        } catch (UnsupportedOperationException | IOException ignored) {
            // Windows or permission issue — System.load will fail with a clear message
        }
    }

    private static String detectOs() {
        String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("linux")) return "linux";
        if (os.contains("mac") || os.contains("darwin")) return "osx";
        if (os.contains("win")) return "windows";
        return os.replaceAll("[^a-z0-9]", "");
    }

    private static String detectArch() {
        String arch = System.getProperty("os.arch", "").toLowerCase();
        return switch (arch) {
            case "amd64", "x86_64" -> "x86_64";
            case "aarch64", "arm64" -> "aarch64";
            default -> arch;
        };
    }

    private static String mapLibraryFileName(String os, String name) {
        return switch (os) {
            case "osx" -> "lib" + name + ".dylib";
            case "windows" -> name + ".dll";
            default -> "lib" + name + ".so";
        };
    }
}
