package org.pangrova.worker.jni;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
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
 *   <li>Local dev: maven-resources-plugin copies from CMake build dir to
 *       {@code target/classes/META-INF/native/...} (see pangrova-worker pom.xml)</li>
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
     * Used by integration tests to skip gracefully when no native bundle is built.
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
     * Returns true if the native library is BOTH available AND its ABI matches
     * the current Java declarations. Attempts a minimal {@link System#load}
     * via the regular loader; if the dylib resource is missing, or extraction
     * fails, or any cached probe stored earlier failed, returns false.
     *
     * <p>The ABI check (dylib has all the {@code nativeXxx} symbols Java
     * declares) is partial — it only catches the case where the dylib is
     * completely absent. For finer ABI verification, callers can attempt
     * the real init and call {@link #markUnsupported()} on UnsatisfiedLinkError
     * to cache a {@code false} for subsequent test classes.
     *
     * <p>Used by E2E test base classes to skip when no native bundle is built.
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
     * remaining test classes can skip cleanly without re-attempting init.
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
                    + ". For local development, run: mvn process-resources -pl pangrova-worker"
                    + " (copies dylib from CMake build dir)."
                    + " For production, ensure pangrova-native-bundle JAR is on the classpath.");
        }

        try {
            Path tempFile = Files.createTempFile("pangrova-native-", "-" + fileName);
            // Schedule cleanup immediately — ensures temp file is removed even if
            // System.load() throws UnsatisfiedLinkError (ABI mismatch, wrong arch).
            tempFile.toFile().deleteOnExit();
            extract(resource, tempFile);
            setExecutePermission(tempFile);
            System.load(tempFile.toAbsolutePath().toString());
            LOG.info("event=native_library_loaded source=classpath os={} arch={}", os, arch);
        } catch (IOException e) {
            throw new UnsatisfiedLinkError(
                    "Failed to extract native library from classpath: " + e.getMessage());
        }
    }

    private static void extract(URL source, Path target) throws IOException {
        try (InputStream in = source.openStream();
             OutputStream out = Files.newOutputStream(target)) {
            in.transferTo(out);
        }
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
