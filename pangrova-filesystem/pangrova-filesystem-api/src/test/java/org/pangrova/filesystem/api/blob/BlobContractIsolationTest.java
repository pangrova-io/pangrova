package org.pangrova.filesystem.api.blob;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.pangrova.common.api.annotation.Stable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlobContractIsolationTest {
    @Test
    void actualApiClasspathAndMinimalLoaderContainNoSdkOrProvider() throws Exception {
        List<String> unavailable = List.of("software.amazon.awssdk.services.s3.S3Client",
                "com.aliyun.sdk.service.oss2.OSSClient", "org.pangrova.filesystem.oss.blob.OssBlobStorage",
                "com.fasterxml.jackson.databind.ObjectMapper", "org.postgresql.Driver",
                "org.pangrova.filesystem.s3.blob.S3BlobStorage",
                "org.pangrova.filesystem.local.blob.FileSystemBlobStorage");
        for (String name : unavailable) {
            assertThatThrownBy(() -> Class.forName(name)).isInstanceOf(ClassNotFoundException.class);
        }
        URL[] sources = {BlobStorage.class.getProtectionDomain().getCodeSource().getLocation(),
                Stable.class.getProtectionDomain().getCodeSource().getLocation()};
        try (var loader = new URLClassLoader(sources, ClassLoader.getPlatformClassLoader())) {
            for (Class<?> contract : List.of(BlobStorage.class, BlobStorageFactory.class, ManagedBlobStorage.class,
                    BlobObjectId.class, BlobInputStream.class)) {
                Class<?> isolated = loader.loadClass(contract.getName());
                assertThat(isolated.getClassLoader()).isSameAs(loader);
                isolated.getDeclaredMethods();
                isolated.getConstructors();
                isolated.getRecordComponents();
                isolated.getAnnotations();
            }
            for (String name : unavailable) {
                assertThatThrownBy(() -> loader.loadClass(name)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }

    @Test
    void objectIdentityRejectsPathsMalformedHashesAndNegativeLengths() {
        for (String token : List.of("", "../escape", "/outside", "A".repeat(64), "a".repeat(63))) {
            assertThatThrownBy(() -> new BlobObjectId(token, "b".repeat(64), 1))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new BlobObjectId("a".repeat(64), token, 1))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new BlobObjectId("a".repeat(64), "b".repeat(64), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void abortThenCloseDoesNotDrainOrCloseRejectedBody() throws Exception {
        var aborts = new AtomicInteger();
        var closes = new AtomicInteger();
        var body = new ByteArrayInputStream(new byte[] {1, 2, 3}) {
            @Override public void close() { closes.incrementAndGet(); }
        };
        var stream = new BlobInputStream(body, aborts::incrementAndGet);
        assertThat(stream.read()).isEqualTo(1);
        stream.abort();
        stream.abort();
        stream.close();
        assertThat(aborts).hasValue(1);
        assertThat(closes).hasValue(0);
        assertThat(body.available()).isEqualTo(2);
    }
}
