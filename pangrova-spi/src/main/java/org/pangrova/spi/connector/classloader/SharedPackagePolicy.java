package org.pangrova.spi.connector.classloader;

import org.pangrova.spi.annotation.Experimental;

import java.util.Set;

/**
 * Controls which packages are shared between the engine classloader and
 * connector plugin classloaders.
 *
 * <p>Trino achieves strong version independence by sharing only the SPI +
 * JDK classes. Pangrova historically used a {@link #BROAD} policy that also
 * shares Guava, Hadoop, Jackson, Parquet, and ORC — convenient but it
 * couples connector builds to the engine's transitive dependency versions.
 *
 * <p>This enum lets operators choose the trade-off:
 * <ul>
 *   <li>{@link #BROAD} — current default, maximum compatibility with
 *       connectors that rely on engine-provided libraries.</li>
 *   <li>{@link #NARROW} — shares only pangrova-spi, pangrova-common,
 *       pangrova-protocol, plus logging + Arrow/Substrait (required for the
 *       data plane). Connectors must bundle their own Guava/Hadoop/etc.</li>
 *   <li>{@link #CUSTOM} — operator specifies the exact set of shared
 *       package prefixes via configuration.</li>
 * </ul>
 */
@Experimental(reason = "G-SPI-04: classloader isolation policy is evolving")
public enum SharedPackagePolicy {

    /**
     * Share a broad set of packages: pangrova SPI + Arrow/Substrait + SLF4J +
     * Guava + Hadoop + Parquet + ORC + Jackson. This is the pre-existing
     * behaviour and the current default.
     */
    BROAD,

    /**
     * Share only the minimum: pangrova SPI packages + Arrow/Substrait + SLF4J.
     * Connectors must shade or bundle all other dependencies.
     */
    NARROW,

    /**
     * Share a user-specified set of package prefixes configured via
     * {@code connector.classloader.shared-packages}.
     */
    CUSTOM;

    /** Packages always shared regardless of policy (the SPI contract). */
    public static final Set<String> SPI_PACKAGES = Set.of(
            "org.pangrova.spi", "org.pangrova.common", "org.pangrova.protocol");

    /** Packages shared by both BROAD and NARROW (engine data-plane essentials). */
    public static final Set<String> BASELINE_SHARED_PACKAGES = Set.of(
            "org.apache.arrow", "io.substrait", "org.slf4j");

    /** Additional packages shared only in BROAD mode. */
    public static final Set<String> BROAD_EXTRA_PACKAGES = Set.of(
            "com.google.common", "org.apache.hadoop", "org.apache.parquet",
            "org.apache.orc", "com.fasterxml.jackson");

    /**
     * Returns the effective shared-package prefixes for this policy.
     *
     * @param customPackages the custom package set; only used when the policy
     *                       is {@link #CUSTOM}, may be {@code null} otherwise
     * @return an unmodifiable set of package prefixes
     * @throws IllegalStateException if the policy is CUSTOM and
     *                               {@code customPackages} is null or empty
     */
    public Set<String> resolveSharedPackages(Set<String> customPackages) {
        return switch (this) {
            case NARROW -> BASELINE_SHARED_PACKAGES;
            case BROAD -> {
                var merged = new java.util.HashSet<>(BASELINE_SHARED_PACKAGES);
                merged.addAll(BROAD_EXTRA_PACKAGES);
                yield Set.copyOf(merged);
            }
            case CUSTOM -> {
                if (customPackages == null || customPackages.isEmpty()) {
                    throw new IllegalStateException(
                            "SharedPackagePolicy.CUSTOM requires a non-empty "
                                + "set of custom packages (connector.classloader.shared-packages)");
                }
                var merged = new java.util.HashSet<>(BASELINE_SHARED_PACKAGES);
                merged.addAll(customPackages);
                yield Set.copyOf(merged);
            }
        };
    }
}
