package org.pangrova.coordinator.property.bootstrap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig.TransportConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property tests for coordinator.transport.advertisedHost configuration.
 *
 * <p>Verifies that the advertised host is correctly propagated through
 * TransportConfig and that loopback detection works for all input shapes.</p>
 *
 * <p><b>Validates: Requirements A3.2</b></p>
 */
class AdvertisedHostPropertyTest {

    @Property(tries = 10)
    void nonLoopbackHostIsNeverDetectedAsLoopback(
            @ForAll("nonLoopbackHosts") String host) {
        TransportConfig config = new TransportConfig(
                0, 256L * 1024 * 1024, host, null);
        assertThat(config.isLoopbackAdvertisedHost()).isFalse();
        assertThat(config.advertisedHost()).isEqualTo(host.trim());
    }

    @Property(tries = 10)
    void loopbackHostsAreAlwaysDetected(
            @ForAll("loopbackHosts") String host) {
        TransportConfig config = new TransportConfig(
                0, 256L * 1024 * 1024, host, null);
        assertThat(config.isLoopbackAdvertisedHost()).isTrue();
    }

    @Property(tries = 10)
    void advertisedHostPreservedInConfig(
            @ForAll("validHosts") String host) {
        TransportConfig config = new TransportConfig(
                8080, 128L * 1024 * 1024, host, null);
        assertThat(config.advertisedHost()).isEqualTo(host.trim());
        assertThat(config.port()).isEqualTo(8080);
        assertThat(config.allocatorLimitBytes()).isEqualTo(128L * 1024 * 1024);
    }

    @Property(tries = 10)
    void defaultsFactoryUsesLocalhost() {
        TransportConfig config = TransportConfig.defaults();
        assertThat(config.advertisedHost()).isEqualTo("localhost");
        assertThat(config.isLoopbackAdvertisedHost()).isTrue();
    }

    @Property(tries = 10)
    void blankAdvertisedHostRejected(
            @ForAll("blankStrings") String blank) {
        assertThatThrownBy(() -> new TransportConfig(
                0, 256L * 1024 * 1024, blank, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("advertisedHost");
    }

    @Property(tries = 10)
    void nullAdvertisedHostRejected() {
        assertThatThrownBy(() -> new TransportConfig(
                0, 256L * 1024 * 1024, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Property(tries = 10)
    void wildcardBindAddressCannotBeAdvertised(@ForAll("wildcardHosts") String host) {
        assertThatThrownBy(() -> new TransportConfig(0, 256L * 1024 * 1024, host, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("wildcard bind address");
    }

    @Provide
    Arbitrary<String> wildcardHosts() {
        return Arbitraries.of("0.0.0.0", "::", "[::]", "0:0:0:0:0:0:0:0", "*", " 0.0.0.0 ");
    }

    @Provide
    Arbitrary<String> nonLoopbackHosts() {
        return Arbitraries.of(
                "10.0.0.1",
                "192.168.1.100",
                "coordinator.prod.internal",
                "my-coordinator-host",
                "172.16.0.5",
                "coordinator-0.coordinator.default.svc.cluster.local",
                "host.docker.internal",
                "2001:db8::1"
        );
    }

    @Provide
    Arbitrary<String> loopbackHosts() {
        return Arbitraries.of(
                "localhost",
                "LOCALHOST",
                "Localhost",
                "127.0.0.1",
                "::1",
                " localhost ",
                " 127.0.0.1 "
        );
    }

    @Provide
    Arbitrary<String> validHosts() {
        return Arbitraries.oneOf(nonLoopbackHosts(), loopbackHosts());
    }

    @Provide
    Arbitrary<String> blankStrings() {
        return Arbitraries.of("", "   ", "\t", "\n");
    }
}
