package org.pangrova.storage.filesystem.object;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.common.storage.StorageType;
import org.pangrova.file.api.object.ObjectStoreAccessorProvider;
import org.pangrova.file.api.profile.StorageProfile;
import org.pangrova.storage.api.blob.BlobStorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSystemObjectProviderIsolationTest {
    @TempDir Path directory;

    @Test
    void allFilesystemTypesLoadWithOnlyTheirTwoContractsAndCommon() throws Exception {
        var unavailable = List.of("software.amazon.awssdk.services.s3.S3Client",
                "com.aliyun.sdk.service.oss2.OSSClient", "org.postgresql.Driver",
                "org.pangrova.worker.file.ObjectStoreAccessorFactory",
                "org.pangrova.file.core.space.FileSpaceAccessService",
                "org.pangrova.storage.s3.object.S3ObjectStoreAccessorProvider");
        for (String name : unavailable) {
            assertThatThrownBy(() -> Class.forName(name)).isInstanceOf(ClassNotFoundException.class);
        }
        URL provider = FileSystemObjectStoreAccessorProvider.class.getProtectionDomain().getCodeSource().getLocation();
        URL file = ObjectStoreAccessorProvider.class.getProtectionDomain().getCodeSource().getLocation();
        URL storage = BlobStorage.class.getProtectionDomain().getCodeSource().getLocation();
        URL common = Experimental.class.getProtectionDomain().getCodeSource().getLocation();
        Path classes = Path.of(provider.toURI());
        try (var loader = new URLClassLoader(new URL[]{provider, file, storage, common},
                ClassLoader.getPlatformClassLoader());
                var files = Files.walk(classes.resolve("org/pangrova/storage/filesystem"))) {
            var names = files.filter(path -> path.toString().endsWith(".class"))
                    .filter(path -> !path.getFileName().toString().equals("package-info.class"))
                    .map(path -> classes.relativize(path).toString().replace(java.io.File.separatorChar, '.')
                            .replaceFirst("\\.class$", "")).toList();
            assertThat(names).isNotEmpty();
            for (String name : names) {
                Class<?> type = loader.loadClass(name);
                assertThat(type.getClassLoader()).isSameAs(loader);
                type.getDeclaredMethods();
                type.getDeclaredConstructors();
                type.getRecordComponents();
            }
            for (String name : unavailable) {
                assertThatThrownBy(() -> loader.loadClass(name)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }

    @Test
    void cachedAndNewProfilesCannotReopenAClosedProviderAndOtherBackendsAreRejected() {
        var provider = new FileSystemObjectStoreAccessorProvider();
        var local = new StorageProfile("local", StorageType.LOCAL, directory.toString(), "", "", "",
                Map.of(), Map.of());
        assertThat(provider.accessor(local)).isSameAs(provider.accessor(local));
        var cloud = new StorageProfile("s3", StorageType.S3, "bucket", "", "", "", Map.of(), Map.of());
        assertThatThrownBy(() -> provider.accessor(cloud)).hasMessageContaining("cannot serve");
        provider.close();
        provider.close();
        assertThatThrownBy(() -> provider.accessor(local)).hasMessageContaining("closed");
        assertThatThrownBy(() -> provider.accessor(cloud)).hasMessageContaining("closed");
    }
}
