package org.pangrova.testing.testkit.network;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.NetworkInterface;

/** Actual local addresses for fixtures that exercise production Worker registration. */
public final class TestNetworkAddresses {
    private TestNetworkAddresses() {}

    /** Selects an active non-loopback IPv4 address, or fails without advertising an unusable substitute. */
    public static Inet4Address routableIpv4() throws IOException {
        var interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces != null && interfaces.hasMoreElements()) {
            var network = interfaces.nextElement();
            if (!network.isUp() || network.isLoopback() || network.isVirtual()) continue;
            var addresses = network.getInetAddresses();
            while (addresses.hasMoreElements()) {
                var address = addresses.nextElement();
                if (address instanceof Inet4Address ipv4 && !ipv4.isLoopbackAddress()
                        && !ipv4.isLinkLocalAddress() && !ipv4.isAnyLocalAddress()) {
                    return ipv4;
                }
            }
        }
        throw new IllegalStateException("No routable local IPv4 address for Worker registration");
    }
}
