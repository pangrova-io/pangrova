package org.pangrova.worker.ai.blob;

import java.net.InetAddress;

/** Classifies resolved endpoints for fail-closed HTTPS import admission. */
final class PublicNetworkAddressPolicy {

    private static final int IPV4_LENGTH = 4;
    private static final int IPV6_LENGTH = 16;
    private static final int IPV4_THIS_NETWORK = 0;
    private static final int IPV4_PRIVATE_A = 10;
    private static final int IPV4_LOOPBACK = 127;
    private static final int IPV4_LINK_LOCAL = 169;
    private static final int IPV4_LINK_LOCAL_SECOND = 254;
    private static final int IPV4_PRIVATE_B = 172;
    private static final int IPV4_PRIVATE_B_MIN = 16;
    private static final int IPV4_PRIVATE_B_MAX = 31;
    private static final int IPV4_PROTOCOL_ASSIGNMENT = 192;
    private static final int IPV4_PROTOCOL_SECOND = 0;
    private static final int IPV4_PRIVATE_C_SECOND = 168;
    private static final int IPV4_SHARED = 100;
    private static final int IPV4_SHARED_MIN = 64;
    private static final int IPV4_SHARED_MAX = 127;
    private static final int IPV4_BENCHMARK = 198;
    private static final int IPV4_BENCHMARK_MIN = 18;
    private static final int IPV4_BENCHMARK_MAX = 19;
    private static final int IPV4_MULTICAST_MIN = 224;
    private static final int IPV6_GLOBAL_MIN = 0x20;
    private static final int IPV6_GLOBAL_MAX = 0x3f;
    private static final int IPV6_PREFIX_2001_SECOND = 0x01;
    private static final int IPV6_DOCUMENTATION_THIRD = 0x0d;
    private static final int IPV6_DOCUMENTATION_FOURTH = 0xb8;
    private static final int IPV6_SPECIAL_THIRD = 0;
    private static final int IPV6_SPECIAL_0200 = 0x02;
    private static final int IPV6_SPECIAL_1000 = 0x10;
    private static final int IPV6_SPECIAL_2000 = 0x20;

    private PublicNetworkAddressPolicy() {}

    static boolean isPublic(InetAddress address) {
        if (isJvmClassifiedNonPublic(address)) {
            return false;
        }
        byte[] bytes = address.getAddress();
        if (bytes.length == IPV4_LENGTH) {
            return isPublicIpv4(bytes);
        }
        return bytes.length == IPV6_LENGTH && isPublicIpv6(bytes);
    }

    private static boolean isJvmClassifiedNonPublic(InetAddress address) {
        return address.isAnyLocalAddress()
                || address.isLoopbackAddress()
                || address.isLinkLocalAddress()
                || address.isSiteLocalAddress()
                || address.isMulticastAddress();
    }

    private static boolean isPublicIpv4(byte[] bytes) {
        int first = unsigned(bytes[0]);
        int second = unsigned(bytes[1]);
        return !isSingleOctetIpv4Range(first)
                && !matches(first, second, IPV4_LINK_LOCAL, IPV4_LINK_LOCAL_SECOND)
                && !inSecondOctetRange(
                        first, second, IPV4_PRIVATE_B,
                        IPV4_PRIVATE_B_MIN, IPV4_PRIVATE_B_MAX)
                && !isProtocolOrPrivateC(first, second)
                && !inSecondOctetRange(
                        first, second, IPV4_SHARED,
                        IPV4_SHARED_MIN, IPV4_SHARED_MAX)
                && !inSecondOctetRange(
                        first, second, IPV4_BENCHMARK,
                        IPV4_BENCHMARK_MIN, IPV4_BENCHMARK_MAX);
    }

    private static boolean isSingleOctetIpv4Range(int first) {
        return first == IPV4_THIS_NETWORK
                || first == IPV4_PRIVATE_A
                || first == IPV4_LOOPBACK
                || first >= IPV4_MULTICAST_MIN;
    }

    private static boolean isProtocolOrPrivateC(int first, int second) {
        return first == IPV4_PROTOCOL_ASSIGNMENT
                && (second == IPV4_PROTOCOL_SECOND
                        || second == IPV4_PRIVATE_C_SECOND);
    }

    private static boolean matches(
            int first, int second, int expectedFirst, int expectedSecond) {
        return first == expectedFirst && second == expectedSecond;
    }

    private static boolean inSecondOctetRange(
            int first, int second, int expectedFirst, int minimum, int maximum) {
        return first == expectedFirst && second >= minimum && second <= maximum;
    }

    private static boolean isPublicIpv6(byte[] bytes) {
        int first = unsigned(bytes[0]);
        return first >= IPV6_GLOBAL_MIN
                && first <= IPV6_GLOBAL_MAX
                && !isDocumentationIpv6(bytes)
                && !isSpecial2001Ipv6(bytes);
    }

    private static boolean isDocumentationIpv6(byte[] bytes) {
        return has2001Prefix(bytes)
                && unsigned(bytes[2]) == IPV6_DOCUMENTATION_THIRD
                && unsigned(bytes[3]) == IPV6_DOCUMENTATION_FOURTH;
    }

    private static boolean isSpecial2001Ipv6(byte[] bytes) {
        if (!has2001Prefix(bytes) || unsigned(bytes[2]) != IPV6_SPECIAL_THIRD) {
            return false;
        }
        int fourth = unsigned(bytes[3]);
        return fourth == IPV6_SPECIAL_0200
                || fourth == IPV6_SPECIAL_1000
                || fourth == IPV6_SPECIAL_2000;
    }

    private static boolean has2001Prefix(byte[] bytes) {
        return unsigned(bytes[0]) == IPV6_GLOBAL_MIN
                && unsigned(bytes[1]) == IPV6_PREFIX_2001_SECOND;
    }

    private static int unsigned(byte value) {
        return Byte.toUnsignedInt(value);
    }
}
