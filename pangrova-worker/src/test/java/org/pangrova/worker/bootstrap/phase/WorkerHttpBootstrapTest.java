package org.pangrova.worker.bootstrap.phase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.ServiceLoader;

import org.pangrova.filesystem.api.blob.BlobStorageFactory;
import org.pangrova.worker.bootstrap.config.WorkerBootstrapConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies {@link WorkerHttpBootstrap#resolveAdvertisedHost}: the address handed to
 * the coordinator and to remote peers must be routable, so {@code worker.host}
 * (a bind address defaulting to the wildcard {@code 0.0.0.0}) must never leak into
 * it (R7.3).
 */
class WorkerHttpBootstrapTest {

    @Test
    void flightTlsWithoutCopyPortDoesNotCreatePrivateListener() {
        var flightOnly = new WorkerBootstrapConfig.TransportSecurityConfig(
                true, "/cert.pem", "/key.pem", "/ca.pem", "worker-1");
        assertThat(WorkerHttpBootstrap.copyTaskTls(flightOnly)).isNull();
        var withCopy = new WorkerBootstrapConfig.TransportSecurityConfig(
                true, "/cert.pem", "/key.pem", "/ca.pem", "worker-1", 8443);
        assertThat(WorkerHttpBootstrap.copyTaskTls(withCopy).port()).isEqualTo(8443);
    }

    @Test
    void copyRequiresAnInstalledAdmissionAwareStorageFactory() throws Exception {
        var factories = ServiceLoader.load(BlobStorageFactory.class);
        assertThatThrownBy(() -> WorkerHttpBootstrap.copyStorage(Map.of("file.storage-backend", "local"), factories))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not support managed COPY");
        assertThatThrownBy(() -> WorkerHttpBootstrap.copyStorage(Map.of("file.storage-backend", "missing"), factories))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unsupported FILE storage backend");
        assertThatThrownBy(() -> WorkerHttpBootstrap.copyStorage(Map.of(
                "file.storage-backend", "s3", "file.storage.s3.bucket", "managed-staging",
                "file.storage.s3.region", "us-east-1", "file.storage.s3.sse", "AES256"), factories))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("file.storage.s3.endpoint");
        try (var storage = WorkerHttpBootstrap.copyStorage(Map.of(
                "file.storage-backend", "s3", "file.storage.s3.bucket", "managed-staging",
                "file.storage.s3.region", "us-east-1", "file.storage.s3.sse", "AES256",
                "file.storage.s3.endpoint", "https://files.example.test"), factories)) {
            assertThat(storage.rootUri()).startsWith("s3://managed-staging/");
            assertThat(storage.backendIdentitySha256()).matches("[a-f0-9]{64}");
        }
    }

    @Test
    void partialEndpointStartupPreservesPrimaryFailureAndReleasesEarlierOwners() {
        var failure = new IllegalArgumentException("file-space provider discovery");
        var cleanup = new IOException("close copy staging");
        var closed = new ArrayList<String>();
        WorkerHttpBootstrap.closeAfterFailure(failure, null,
                () -> { closed.add("copy"); throw cleanup; }, () -> closed.add("dataset"));
        assertThat(closed).containsExactly("copy", "dataset");
        assertThat(failure.getSuppressed()).containsExactly(cleanup);
    }

    private static WorkerBootstrapConfig config(String httpHost, String advertisedHost) {
        return new WorkerBootstrapConfig(
                "w-1", 0, 0, null,
                WorkerBootstrapConfig.DEFAULT_HEARTBEAT_INTERVAL_MS,
                false, null, null,
                WorkerBootstrapConfig.DEFAULT_RESULT_RETENTION_SECONDS,
                false, null, httpHost, null, null, null, advertisedHost,
                null,
                null, null, org.pangrova.testing.nativefixture.TestShuffleServiceConfig.admissionDirectory());
    }

    @Test
    void prefersAdvertisedHostOverBindHost() {
        String host = WorkerHttpBootstrap.resolveAdvertisedHost(
                config("0.0.0.0", "worker-3.internal.example.com"));

        assertThat(host).isEqualTo("worker-3.internal.example.com");
    }

    @Test
    void trimsAdvertisedHost() {
        assertThat(WorkerHttpBootstrap.resolveAdvertisedHost(config(null, "  worker-3  ")))
                .isEqualTo("worker-3");
    }

    @Test
    void fallsBackToBindHostWhenItIsConcrete() {
        String host = WorkerHttpBootstrap.resolveAdvertisedHost(
                config("my-custom-host.example.com", null));

        assertThat(host).isEqualTo("my-custom-host.example.com");
    }

    /**
     * Regression guard: advertising the wildcard bind address leaves peers with an
     * address they cannot dial back.
     */
    @ParameterizedTest
    @ValueSource(strings = {"0.0.0.0", "::", "[::]", "::0", "0:0:0:0:0:0:0:0", "*", " 0.0.0.0 "})
    void neverAdvertisesWildcardBindAddress(String wildcard) {
        String host = WorkerHttpBootstrap.resolveAdvertisedHost(config(wildcard, null));

        assertThat(host).isNotBlank();
        assertThat(WorkerHttpBootstrap.isWildcardAddress(host)).isFalse();
    }

    @Test
    void blankAdvertisedHostFallsThroughToBindHost() {
        assertThat(WorkerHttpBootstrap.resolveAdvertisedHost(config("10.0.0.7", "   ")))
                .isEqualTo("10.0.0.7");
    }

    @Test
    void fallsBackToLocalHostnameWhenNothingConfigured() {
        WorkerBootstrapConfig config = WorkerBootstrapConfig.withDefaults("w-2");

        String host = WorkerHttpBootstrap.resolveAdvertisedHost(config);

        // Either the detected InetAddress hostname or the "localhost" fallback.
        assertThat(host).isNotNull().isNotBlank();
    }

    @Test
    void fallsBackToLocalHostnameWhenBindHostBlank() {
        String host = WorkerHttpBootstrap.resolveAdvertisedHost(config("   ", null));

        assertThat(host).isNotBlank();
    }

    @Test
    void isWildcardAddressRejectsConcreteAddresses() {
        assertThat(WorkerHttpBootstrap.isWildcardAddress("127.0.0.1")).isFalse();
        assertThat(WorkerHttpBootstrap.isWildcardAddress("localhost")).isFalse();
        assertThat(WorkerHttpBootstrap.isWildcardAddress("10.0.0.1")).isFalse();
        assertThat(WorkerHttpBootstrap.isWildcardAddress(null)).isFalse();
    }
}
