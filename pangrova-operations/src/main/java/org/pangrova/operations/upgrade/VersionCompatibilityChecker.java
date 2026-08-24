package org.pangrova.operations.upgrade;

import java.util.Map;
import java.util.Objects;

/**
 * Checks version compatibility between nodes during rolling upgrades.
 * Protocol versions must differ by at most 1 for safe upgrade.
 *
 * <p>Two modes are supported:
 * <ul>
 *   <li><strong>Legacy mode</strong> (exact-match): each version string maps to a protocol number</li>
 *   <li><strong>Range mode</strong>: semver range expressions map to protocol numbers,
 *       so patch/minor versions within a range share the same protocol</li>
 * </ul>
 */
public final class VersionCompatibilityChecker {

    private final Map<String, Integer> protocolVersionMap;
    private final Map<String, Integer> rangeProtocolMap;
    private final boolean rangeMode;

    /**
     * Creates a checker in legacy exact-match mode.
     */
    public VersionCompatibilityChecker(Map<String, Integer> protocolVersionMap) {
        this.protocolVersionMap = Objects.requireNonNull(protocolVersionMap, "protocolVersionMap");
        this.rangeProtocolMap = null;
        this.rangeMode = false;
    }

    private VersionCompatibilityChecker(Map<String, Integer> rangeProtocolMap, boolean rangeMode) {
        this.protocolVersionMap = Map.of();
        this.rangeProtocolMap = Objects.requireNonNull(rangeProtocolMap, "rangeProtocolMap");
        this.rangeMode = rangeMode;
    }

    /**
     * Creates a checker in semver range mode. Keys are range expressions
     * (e.g. {@code ">=1.0.0 <2.0.0"}) and values are protocol numbers.
     */
    public static VersionCompatibilityChecker withRanges(Map<String, Integer> rangeProtocolMap) {
        return new VersionCompatibilityChecker(rangeProtocolMap, true);
    }

    /** Returns true if the two versions map to protocol numbers within one step of each other. */
    public boolean isCompatible(String currentVersion, String targetVersion) {
        Integer currentProtocol = resolveProtocol(currentVersion);
        Integer targetProtocol = resolveProtocol(targetVersion);
        if (currentProtocol == null || targetProtocol == null) {
            return false;
        }
        return Math.abs(currentProtocol - targetProtocol) <= 1;
    }

    /**
     * Resolves the protocol number for the given version string.
     * In legacy mode, performs exact lookup. In range mode, tests each range.
     *
     * @return the protocol number, or null if no match
     */
    public Integer resolveProtocol(String version) {
        if (rangeMode) {
            for (Map.Entry<String, Integer> entry : rangeProtocolMap.entrySet()) {
                VersionRange range = VersionRange.parse(entry.getKey());
                if (range.satisfiedBy(version)) {
                    return entry.getValue();
                }
            }
            return null;
        }
        return protocolVersionMap.get(version);
    }

    public boolean isNativeCompatible(String javaVersion, String nativeVersion) {
        int javaMajor = getMajor(javaVersion);
        int nativeMajor = getMajor(nativeVersion);
        return javaMajor == nativeMajor;
    }

    int getMajor(String version) {
        Objects.requireNonNull(version, "version");
        String majorStr = version.split("\\.")[0];
        return Integer.parseInt(majorStr);
    }
}
