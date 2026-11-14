package org.pangrova.testing.integration.e2e.speculation;

import org.testcontainers.DockerClientFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipFile;

/** Owns the frozen production classpath, canonical JNI identity and source provenance. */
record LocalRuntimeArtifacts(String coordinatorClasspath, String workerClasspath, Path cliJar, Path manifestPath,
                             String manifestSha256, String nativeSha256) {
    private static final String IMAGE_ID = "pangrova.e2e.image-id";
    private static final String IMAGE_PROVENANCE = "pangrova.e2e.local.image-provenance";
    private static final String SOURCE_COMMIT = "pangrova.e2e.local.source-commit";
    private static final Set<String> IMAGE_KEYS = Set.of("image.id", "source.commit",
            "classifier", "native.sha256", "bundle.sha256", "manifest.sha256");

    static LocalRuntimeArtifacts fromRequiredProperties() throws Exception {
        Platform platform = Platform.current();
        Path lib = Path.of(required("pangrova.e2e.local.lib-dir")).toRealPath();
        Path nativeLibrary = Path.of(required("pangrova.e2e.local.native-lib")).toRealPath();
        if (!nativeLibrary.getFileName().toString().equals(platform.libraryFile())) {
            throw new IllegalStateException("Native library filename does not match local platform");
        }
        byte[] expected;
        try (var input = Files.newInputStream(nativeLibrary)) {
            expected = digest(input);
        }
        String nativeHash = HexFormat.of().formatHex(expected);
        List<Path> jars = jarInventory(lib);
        Path manifestPath = Path.of(required("pangrova.e2e.local.jar-manifest")).toRealPath();
        Path stage = manifestPath.getParent();
        if (!Files.isSameFile(lib, stage.resolve("lib"))) {
            throw new IllegalStateException("Runtime lib and manifest must share the same staging directory");
        }
        List<Path> cliJars = jarInventory(stage.resolve("cli"));
        Path cli = unique(cliJars, "pangrova-client-cli-", "-exec.jar");
        if (cliJars.size() != 1) throw new IllegalStateException("CLI directory must contain only its executable");
        String manifestHash = hash(manifestPath);
        var inventory = new ArrayList<>(cliJars);
        inventory.addAll(jars);
        var coordinatorJars = roleJars(lib.resolve("coordinator"), inventory);
        var workerJars = roleJars(lib.resolve("worker"), inventory);
        verifyManifest(inventory, manifestPath);
        Path bundle = unique(jars, "pangrova-native-bundle-",
                "-" + platform.classifier() + ".jar");
        String nativeEntry = "META-INF/native/" + platform.resourceOs() + "/"
                + platform.resourceArch() + "/" + platform.libraryFile();
        verifyOrigin(lib, nativeLibrary, manifestPath, bundle, platform.classifier(),
                nativeEntry, nativeHash, manifestHash);
        verifyNativeEntries(jars, bundle, nativeEntry, expected);
        Path workerBundle = unique(workerJars, "pangrova-native-bundle-",
                "-" + platform.classifier() + ".jar");
        verifyNativeEntries(workerJars, workerBundle, nativeEntry, expected);
        return new LocalRuntimeArtifacts(classpath(coordinatorJars), classpath(workerJars),
                cli, manifestPath, manifestHash, nativeHash);
    }

    private static String classpath(List<Path> jars) {
        return String.join(java.io.File.pathSeparator, jars.stream().map(Path::toString).toList());
    }

    private static List<Path> roleJars(Path directory, List<Path> inventory) throws Exception {
        Path list = directory.resolve("classpath.list");
        var jars = jarInventory(directory);
        var ordered = new java.util.LinkedHashSet<Path>();
        for (String name : Files.readAllLines(list)) {
            Path jar = directory.resolve(name);
            if (!name.endsWith(".jar") || name.contains("/") || name.contains(":")
                    || !jars.contains(jar) || !ordered.add(jar)) {
                throw new IllegalStateException("Invalid ordered role classpath entry in " + list + ": " + name);
            }
        }
        if (ordered.size() != jars.size()) {
            throw new IllegalStateException("Ordered role classpath differs from JAR inventory: " + directory);
        }
        inventory.add(list);
        inventory.addAll(jars);
        return List.copyOf(ordered);
    }

    private static List<Path> jarInventory(Path lib) throws Exception {
        try (var paths = Files.list(lib)) {
            List<Path> jars = paths.filter(path -> path.getFileName().toString().endsWith(".jar"))
                    .sorted().toList();
            if (jars.isEmpty()) throw new IllegalStateException("Frozen runtime lib is empty: " + lib);
            return jars;
        }
    }

    private static void verifyManifest(List<Path> jars, Path manifest) throws Exception {
        List<String> actual = new ArrayList<>();
        for (Path jar : jars) actual.add(hash(jar) + "  " + manifest.getParent().relativize(jar));
        if (!Files.readAllLines(manifest).equals(actual)) {
            throw new IllegalStateException("Runtime JARs differ from the frozen SHA-256 manifest");
        }
    }

    private static void verifyNativeEntries(List<Path> jars, Path bundle, String nativeEntry,
                                            byte[] expected) throws Exception {
        int matchingEntries = 0;
        for (Path jar : jars) {
            String name = jar.getFileName().toString();
            if (name.endsWith("-exec.jar")) {
                throw new IllegalStateException("Executable JAR cannot be in the service classpath: " + jar);
            }
            if (name.startsWith("original-") || name.contains("-tests.")
                    || name.contains("-native-fixtures.")) {
                throw new IllegalStateException("Test or original JAR in runtime classpath: " + jar);
            }
            try (var zip = new ZipFile(jar.toFile())) {
                var entry = zip.getEntry(nativeEntry);
                if (jar.equals(bundle) && entry == null) {
                    throw new IllegalStateException("Classifier bundle lacks canonical Native entry: " + jar);
                }
                if (entry == null) continue;
                matchingEntries++;
                byte[] actual;
                try (var input = zip.getInputStream(entry)) {
                    actual = digest(input);
                }
                if (!Arrays.equals(expected, actual)) {
                    throw new IllegalStateException("Stale canonical Native resource in " + jar);
                }
            }
        }
        if (matchingEntries == 0) {
            throw new IllegalStateException("Runtime classpath has no Release Native resource: "
                    + nativeEntry);
        }
    }

    private static void verifyOrigin(Path lib, Path nativeLibrary, Path manifest, Path bundle,
                                     String classifier, String nativeEntry,
                                     String nativeHash, String manifestHash) throws Exception {
        boolean imageMode = System.getProperty(IMAGE_ID) != null
                || System.getProperty(IMAGE_PROVENANCE) != null
                || System.getProperty(SOURCE_COMMIT) != null;
        if (!imageMode) {
            verifyReleaseCache(nativeLibrary);
            return;
        }
        String imageId = required(IMAGE_ID);
        String sourceCommit = required(SOURCE_COMMIT);
        if (!imageId.matches("sha256:[0-9a-f]{64}") || !sourceCommit.matches("[0-9a-f]{40}")) {
            throw new IllegalStateException("Image digest or source commit is not canonical");
        }
        Path receipt = Path.of(required(IMAGE_PROVENANCE)).toRealPath();
        verifyExtractionOwner(lib, nativeLibrary, manifest, receipt, nativeEntry);
        verifyImageReceipt(receipt, bundle, classifier, imageId, sourceCommit, nativeHash, manifestHash);
        verifyDockerIdentity(imageId, sourceCommit);
    }

    private static void verifyReleaseCache(Path nativeLibrary) throws Exception {
        String cache = Files.readString(nativeLibrary.getParent().resolve("CMakeCache.txt"));
        if (!cache.lines().anyMatch(line -> line.equals("CMAKE_BUILD_TYPE:STRING=Release"))) {
            throw new IllegalStateException("The local JNI input is not a Release CMake build");
        }
    }

    private static void verifyExtractionOwner(Path lib, Path nativeLibrary, Path manifest,
                                             Path receipt, String nativeEntry) throws Exception {
        Path stage = receipt.getParent();
        if (!Files.isSameFile(lib, stage.resolve("lib"))
                || !Files.isSameFile(manifest, stage.resolve("runtime.sha256"))
                || !Files.isSameFile(nativeLibrary, stage.resolve("extracted").resolve(nativeEntry))) {
            throw new IllegalStateException("Runtime files do not share the image-extraction owner");
        }
    }

    private static void verifyImageReceipt(Path receipt, Path bundle, String classifier,
                                          String imageId, String sourceCommit,
                                          String nativeHash, String manifestHash) throws Exception {
        Properties evidence = new Properties();
        try (InputStream input = Files.newInputStream(receipt)) {
            evidence.load(input);
        }
        if (!evidence.stringPropertyNames().equals(IMAGE_KEYS)
                || !imageId.equals(evidence.getProperty("image.id"))
                || !sourceCommit.equals(evidence.getProperty("source.commit"))
                || !classifier.equals(evidence.getProperty("classifier"))
                || !nativeHash.equals(evidence.getProperty("native.sha256"))
                || !hash(bundle).equals(evidence.getProperty("bundle.sha256"))
                || !manifestHash.equals(evidence.getProperty("manifest.sha256"))) {
            throw new IllegalStateException("Production image extraction provenance differs from runtime bytes");
        }
    }

    private static void verifyDockerIdentity(String imageId, String sourceCommit) {
        var inspected = DockerClientFactory.instance().client().inspectImageCmd(imageId).exec();
        if (!imageId.equals(inspected.getId()) || inspected.getConfig() == null
                || inspected.getConfig().getLabels() == null
                || !sourceCommit.equals(inspected.getConfig().getLabels()
                        .get("org.opencontainers.image.revision"))) {
            throw new IllegalStateException("Actual Docker image digest or source label differs");
        }
    }

    private static Path unique(List<Path> jars, String prefix, String suffix) {
        var matches = jars.stream().filter(path -> {
            String name = path.getFileName().toString();
            return name.startsWith(prefix) && name.endsWith(suffix);
        }).toList();
        if (matches.size() != 1) {
            throw new IllegalStateException("Expected one runtime JAR " + prefix + "*" + suffix
                    + ", got " + matches);
        }
        return matches.get(0);
    }

    private static String hash(Path file) throws Exception {
        try (InputStream input = Files.newInputStream(file)) {
            return HexFormat.of().formatHex(digest(input));
        }
    }

    private static byte[] digest(InputStream input) throws Exception {
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        byte[] chunk = new byte[64 * 1024];
        for (int size; (size = input.read(chunk)) != -1;) {
            sha.update(chunk, 0, size);
        }
        return sha.digest();
    }

    private static String required(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing exact local process fixture input: " + name);
        }
        return value;
    }

    private record Platform(String resourceOs, String resourceArch,
                            String classifier, String libraryFile) {
        static Platform current() {
            String os = System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT);
            String resourceOs;
            String suffix;
            if (os.contains("mac") || os.contains("darwin")) {
                resourceOs = "osx";
                suffix = ".dylib";
            } else if (os.contains("linux")) {
                resourceOs = "linux";
                suffix = ".so";
            } else {
                throw new IllegalStateException("Unsupported local process platform: " + os);
            }
            String arch = System.getProperty("os.arch", "").toLowerCase(java.util.Locale.ROOT);
            String resourceArch = switch (arch) {
                case "aarch64", "arm64" -> "aarch64";
                case "x86_64", "amd64" -> "x86_64";
                default -> throw new IllegalStateException("Unsupported local process arch: " + arch);
            };
            String classifierArch = resourceArch.equals("aarch64") ? "aarch_64" : resourceArch;
            return new Platform(resourceOs, resourceArch, resourceOs + "-" + classifierArch,
                    "libpangrova_native" + suffix);
        }
    }
}
