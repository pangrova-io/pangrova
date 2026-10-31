package org.pangrova.testing.integration.e2e.speculation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Component checks for admission of frozen local process artifacts; no JNI or Docker is started. */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class LocalRuntimeArtifactsTest {
    private static final String LIB_DIR = "pangrova.e2e.local.lib-dir";
    private static final String NATIVE_LIB = "pangrova.e2e.local.native-lib";
    private static final String JAR_MANIFEST = "pangrova.e2e.local.jar-manifest";
    private static final String IMAGE_ID = "pangrova.e2e.image-id";
    private static final String IMAGE_PROVENANCE = "pangrova.e2e.local.image-provenance";
    private static final String SOURCE_COMMIT = "pangrova.e2e.local.source-commit";
    private static final List<String> PROPERTY_NAMES = List.of(LIB_DIR, NATIVE_LIB, JAR_MANIFEST,
            IMAGE_ID, IMAGE_PROVENANCE, SOURCE_COMMIT);
    private static final byte[] NATIVE_BYTES = "release-native-bytes".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    @TempDir
    Path directory;

    private final Map<String, String> priorProperties = new HashMap<>();
    private Path libDirectory;
    private Path nativeLibrary;
    private Path manifest;
    private Path bundle;
    private Path cli;
    private Path thinCli;
    private Path worker;
    private String nativeEntry;

    @BeforeEach
    void createFrozenFixture() throws Exception {
        for (String property : PROPERTY_NAMES) {
            priorProperties.put(property, System.getProperty(property));
            System.clearProperty(property);
        }
        directory = directory.toRealPath();
        String os = System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT)
                .contains("mac") ? "osx" : "linux";
        String architecture = System.getProperty("os.arch").toLowerCase(java.util.Locale.ROOT);
        String resourceArch = architecture.equals("arm64") || architecture.equals("aarch64")
                ? "aarch64" : "x86_64";
        String classifierArch = resourceArch.equals("aarch64") ? "aarch_64" : resourceArch;
        String libraryFile = "libpangrova_native" + (os.equals("osx") ? ".dylib" : ".so");
        nativeEntry = "META-INF/native/" + os + "/" + resourceArch + "/" + libraryFile;

        libDirectory = Files.createDirectory(directory.resolve("lib"));
        Path nativeDirectory = Files.createDirectory(directory.resolve("native"));
        nativeLibrary = Files.write(nativeDirectory.resolve(libraryFile), NATIVE_BYTES);
        Files.writeString(nativeDirectory.resolve("CMakeCache.txt"), "CMAKE_BUILD_TYPE:STRING=Release\n");
        bundle = libDirectory.resolve("pangrova-native-bundle-0.1.0-SNAPSHOT-"
                + os + "-" + classifierArch + ".jar");
        cli = Files.createDirectory(directory.resolve("cli")).resolve("pangrova-client-cli-0.1.0-SNAPSHOT-exec.jar");
        thinCli = libDirectory.resolve("pangrova-client-cli-0.1.0-SNAPSHOT.jar");
        worker = libDirectory.resolve("pangrova-worker-core-0.1.0-SNAPSHOT.jar");
        writeJar(bundle, Map.of(nativeEntry, NATIVE_BYTES));
        writeJar(cli, Map.of("cli.txt", new byte[] {1}));
        writeJar(thinCli, Map.of("thin-cli.txt", new byte[] {1}));
        writeJar(worker, Map.of(nativeEntry, NATIVE_BYTES));
        manifest = directory.resolve("runtime.sha256");
        refreshManifest();
        System.setProperty(LIB_DIR, libDirectory.toString());
        System.setProperty(NATIVE_LIB, nativeLibrary.toString());
        System.setProperty(JAR_MANIFEST, manifest.toString());
    }

    @AfterEach
    void restoreProperties() {
        for (String property : PROPERTY_NAMES) {
            String value = priorProperties.get(property);
            if (value == null) System.clearProperty(property);
            else System.setProperty(property, value);
        }
    }

    @Test
    void admitsMatchingReleaseInventoryWithoutPuttingShadedCliOnMainClasspath() throws Exception {
        LocalRuntimeArtifacts artifacts = LocalRuntimeArtifacts.fromRequiredProperties();

        assertThat(artifacts.nativeSha256()).isEqualTo(hash(nativeLibrary));
        assertThat(artifacts.manifestSha256()).isEqualTo(hash(manifest));
        assertThat(artifacts.manifestPath()).isEqualTo(manifest);
        assertThat(artifacts.cliJar()).isEqualTo(cli);
        assertThat(artifacts.classpath().split(java.io.File.pathSeparator))
                .containsExactly(bundle.toString(), thinCli.toString(), worker.toString());
    }

    @Test
    void refusesEachMissingRequiredPath() {
        for (String property : List.of(LIB_DIR, NATIVE_LIB, JAR_MANIFEST)) {
            String configured = System.getProperty(property);
            System.clearProperty(property);
            try {
                assertThatThrownBy(LocalRuntimeArtifacts::fromRequiredProperties)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Missing exact local process fixture input: " + property);
            } finally {
                System.setProperty(property, configured);
            }
        }
    }

    @Test
    void refusesJarBytesChangedAfterFrozenManifest() throws Exception {
        writeJar(worker, Map.of("changed.txt", new byte[] {9}));

        assertThatThrownBy(LocalRuntimeArtifacts::fromRequiredProperties)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Runtime JARs differ from the frozen SHA-256 manifest");
    }

    @Test
    void refusesExecutableChangedAfterFrozenManifest() throws Exception {
        writeJar(cli, Map.of("changed-cli.txt", new byte[] {9}));
        assertThatThrownBy(LocalRuntimeArtifacts::fromRequiredProperties)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Runtime JARs differ from the frozen SHA-256 manifest");
    }

    @Test
    void refusesExecutableInServiceClasspathEvenWithUpdatedManifest() throws Exception {
        Files.copy(cli, libDirectory.resolve(cli.getFileName()));
        refreshManifest();
        assertThatThrownBy(LocalRuntimeArtifacts::fromRequiredProperties)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Executable JAR cannot be in the service classpath");
    }

    @Test
    void refusesStaleCanonicalNativeEntryEvenWithUpdatedManifest() throws Exception {
        writeJar(worker, Map.of(nativeEntry, "stale-native".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        refreshManifest();

        assertThatThrownBy(LocalRuntimeArtifacts::fromRequiredProperties)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Stale canonical Native resource in " + worker);
    }

    @Test
    void refusesTestsJarEvenWhenIncludedInFrozenManifest() throws Exception {
        writeJar(libDirectory.resolve("pangrova-worker-core-0.1.0-SNAPSHOT-tests.jar"),
                Map.of("test.txt", new byte[] {1}));
        refreshManifest();

        assertThatThrownBy(LocalRuntimeArtifacts::fromRequiredProperties)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Test or original JAR in runtime classpath");
    }

    @Test
    void refusesNonReleaseCmakeCache() throws Exception {
        Files.writeString(nativeLibrary.getParent().resolve("CMakeCache.txt"),
                "CMAKE_BUILD_TYPE:STRING=Debug\n");

        assertThatThrownBy(LocalRuntimeArtifacts::fromRequiredProperties)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not a Release CMake build");
    }

    @Test
    void refusesReleasePrefixedBuildTypeRatherThanUsingSubstringAdmission() throws Exception {
        Files.writeString(nativeLibrary.getParent().resolve("CMakeCache.txt"),
                "CMAKE_BUILD_TYPE:STRING=ReleaseWithDebInfo\n");

        assertThatThrownBy(LocalRuntimeArtifacts::fromRequiredProperties)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not a Release CMake build");
    }

    @Test
    void refusesIncompleteImageProvenanceWithoutFallingBackToLocalRelease() {
        System.setProperty(IMAGE_ID, "sha256:" + "a".repeat(64));

        assertThatThrownBy(LocalRuntimeArtifacts::fromRequiredProperties)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing exact local process fixture input: " + SOURCE_COMMIT);
    }

    private void refreshManifest() throws Exception {
        List<String> lines = new ArrayList<>();
        lines.add(hash(cli) + "  " + directory.relativize(cli));
        try (var paths = Files.list(libDirectory)) {
            for (Path jar : paths.filter(path -> path.getFileName().toString().endsWith(".jar"))
                    .sorted().toList()) {
                lines.add(hash(jar) + "  " + directory.relativize(jar));
            }
        }
        Files.writeString(manifest, String.join("\n", lines) + "\n");
    }

    private static void writeJar(Path target, Map<String, byte[]> entries) throws Exception {
        try (var output = new JarOutputStream(Files.newOutputStream(target))) {
            for (var entry : entries.entrySet()) {
                output.putNextEntry(new JarEntry(entry.getKey()));
                output.write(entry.getValue());
                output.closeEntry();
            }
        }
    }

    private static String hash(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(file)) {
            byte[] chunk = new byte[8192];
            for (int length; (length = input.read(chunk)) != -1;) {
                digest.update(chunk, 0, length);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
