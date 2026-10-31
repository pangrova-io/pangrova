package org.pangrova.worker.nativebridge.jni;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real classpath resource extraction and conflict checks, not JNI execution. */
class NativeLibraryResourceIdentityTest {
    private static final String RESOURCE = "META-INF/native/osx/aarch64/libpangrova_native.dylib";

    @TempDir Path directory;

    @Test
    void singleCanonicalResourceIsCopiedWithoutWholeLibraryBuffering() throws Exception {
        byte[] bytes = new byte[8192 * 2 + 31];
        bytes[bytes.length - 1] = 42;
        Path first = resource("first", bytes);
        try (var loader = loader(first)) {
            Path output = directory.resolve("extracted");
            NativeLibraryLoader.extractVerifiedResource(loader, RESOURCE, loader.getResource(RESOURCE), output);
            assertThat(Files.readAllBytes(output)).isEqualTo(bytes);
        }
    }

    @Test
    void identicalArtifactsDoNotCreateASecondLoadingPath() throws Exception {
        byte[] bytes = {1, 2, 3, 4};
        Path first = resource("first", bytes);
        Path second = resource("second", bytes);
        try (var loader = loader(first, second)) {
            Path output = directory.resolve("extracted");
            NativeLibraryLoader.extractVerifiedResource(loader, RESOURCE, loader.getResource(RESOURCE), output);
            assertThat(Files.readAllBytes(output)).isEqualTo(bytes);
        }
    }

    @Test
    void conflictingArtifactFailsBeforeNativeLoadingRegardlessOfClasspathOrder() throws Exception {
        Path first = resource("first", new byte[] {1, 2, 3, 4});
        Path second = resource("second", new byte[] {1, 2, 3, 5});
        for (Path[] order : new Path[][] {{first, second}, {second, first}}) {
            try (var loader = loader(order)) {
                assertThatThrownBy(() -> NativeLibraryLoader.extractVerifiedResource(
                        loader, RESOURCE, loader.getResource(RESOURCE), directory.resolve("extracted")))
                        .isInstanceOf(UnsatisfiedLinkError.class)
                        .hasMessageContaining("Conflicting native libraries")
                        .hasMessageContaining(RESOURCE);
            }
        }
    }

    private Path resource(String name, byte[] bytes) throws Exception {
        Path root = directory.resolve(name);
        Path resource = root.resolve(RESOURCE);
        Files.createDirectories(resource.getParent());
        Files.write(resource, bytes);
        return root;
    }

    private static URLClassLoader loader(Path... roots) throws Exception {
        URL[] urls = new URL[roots.length];
        for (int index = 0; index < roots.length; index++) {
            urls[index] = roots[index].toUri().toURL();
        }
        return new URLClassLoader(urls, null);
    }
}
