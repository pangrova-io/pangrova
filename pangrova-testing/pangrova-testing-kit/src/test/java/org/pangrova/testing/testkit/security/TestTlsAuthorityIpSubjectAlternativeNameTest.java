package org.pangrova.testing.testkit.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTlsAuthorityIpSubjectAlternativeNameTest {
    @TempDir Path workspace;

    @Test
    void ipOverloadIssuesExactIpSanWhileDnsOverloadRetainsDnsSan() throws Exception {
        InetAddress address = InetAddress.getByAddress(new byte[] {(byte) 192, 0, 2, 42});
        try (var authority = new TestTlsAuthority(workspace)) {
            var ipIdentity = authority.issue("worker-ip", address, false);
            var dnsIdentity = authority.issue("worker-dns", "localhost", false);
            X509Certificate ip = certificate(ipIdentity.certificateChain());
            X509Certificate dns = certificate(dnsIdentity.certificateChain());

            assertThat(ip.getSubjectAlternativeNames()).containsExactly(List.of(7, "192.0.2.42"));
            assertThat(dns.getSubjectAlternativeNames()).containsExactly(List.of(2, "localhost"));
            assertThat(ip.getExtendedKeyUsage()).containsExactlyInAnyOrder(
                    "1.3.6.1.5.5.7.3.1", "1.3.6.1.5.5.7.3.2");
            assertThatThrownBy(() -> authority.issue("worker-loopback",
                    InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), false))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void mappedPortCertificateRetainsTheContainerDnsAndLoopbackIp() throws Exception {
        InetAddress loopback = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
        try (var authority = new TestTlsAuthority(workspace)) {
            var identity = authority.issueForMappedEndpoint("worker", "worker-1", loopback);
            assertThat(certificate(identity.certificateChain()).getSubjectAlternativeNames())
                    .containsExactlyInAnyOrder(List.of(2, "worker-1"), List.of(7, "127.0.0.1"));
        }
    }

    private static X509Certificate certificate(Path path) throws Exception {
        try (var input = Files.newInputStream(path)) {
            return (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
    }
}
