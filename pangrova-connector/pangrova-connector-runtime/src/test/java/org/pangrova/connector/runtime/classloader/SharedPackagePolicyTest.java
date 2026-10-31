package org.pangrova.connector.runtime.classloader;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link SharedPackagePolicy} — verifies that each policy mode
 * resolves the correct set of shared package prefixes.
 */
class SharedPackagePolicyTest {

    @Test
    void narrowPolicySharesOnlyBaseline() {
        Set<String> packages = SharedPackagePolicy.NARROW.resolveSharedPackages(null);

        assertThat(packages).containsExactlyInAnyOrderElementsOf(
                SharedPackagePolicy.BASELINE_SHARED_PACKAGES);
        // Must NOT include broad extras
        assertThat(packages).doesNotContainAnyElementsOf(
                SharedPackagePolicy.BROAD_EXTRA_PACKAGES);
    }

    @Test
    void broadPolicySharesBaselinePlusBroadExtras() {
        Set<String> packages = SharedPackagePolicy.BROAD.resolveSharedPackages(null);

        assertThat(packages).containsAll(SharedPackagePolicy.BASELINE_SHARED_PACKAGES);
        assertThat(packages).containsAll(SharedPackagePolicy.BROAD_EXTRA_PACKAGES);
    }

    @Test
    void broadPolicyIncludesGuavaHadoopJacksonParquetOrc() {
        Set<String> packages = SharedPackagePolicy.BROAD.resolveSharedPackages(null);

        assertThat(packages).contains(
                "com.google.common",
                "org.apache.hadoop",
                "com.fasterxml.jackson",
                "org.apache.parquet",
                "org.apache.orc");
    }

    @Test
    void customPolicyMergesBaselineWithUserPackages() {
        Set<String> custom = Set.of("com.example.mylib", "io.mycompany.utils");

        Set<String> packages = SharedPackagePolicy.CUSTOM.resolveSharedPackages(custom);

        // Must contain baseline
        assertThat(packages).containsAll(SharedPackagePolicy.BASELINE_SHARED_PACKAGES);
        // Must contain user-specified packages
        assertThat(packages).contains("com.example.mylib", "io.mycompany.utils");
        // Must NOT contain broad extras (those were not requested)
        assertThat(packages).doesNotContain("com.google.common", "org.apache.hadoop");
    }

    @Test
    void customPolicyWithNullPackagesThrows() {
        assertThatThrownBy(() -> SharedPackagePolicy.CUSTOM.resolveSharedPackages(null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("non-empty");
    }

    @Test
    void customPolicyWithEmptyPackagesThrows() {
        assertThatThrownBy(() -> SharedPackagePolicy.CUSTOM.resolveSharedPackages(Set.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("non-empty");
    }

    @Test
    void spiPackagesContainsCoreModules() {
        assertThat(SharedPackagePolicy.SPI_PACKAGES).containsExactlyInAnyOrder(
                "org.pangrova.connector.spi",
                "org.pangrova.common", "org.pangrova.protocol");
    }

    @Test
    void baselineSharedPackagesContainsArrowSubstraitSlf4j() {
        assertThat(SharedPackagePolicy.BASELINE_SHARED_PACKAGES).containsExactlyInAnyOrder(
                "org.apache.arrow", "io.substrait", "org.slf4j");
    }

    @Test
    void resolvedSetsAreUnmodifiable() {
        Set<String> broad = SharedPackagePolicy.BROAD.resolveSharedPackages(null);
        assertThatThrownBy(() -> broad.add("foo"))
                .isInstanceOf(UnsupportedOperationException.class);

        Set<String> narrow = SharedPackagePolicy.NARROW.resolveSharedPackages(null);
        assertThatThrownBy(() -> narrow.add("foo"))
                .isInstanceOf(UnsupportedOperationException.class);

        Set<String> custom = SharedPackagePolicy.CUSTOM.resolveSharedPackages(Set.of("x.y"));
        assertThatThrownBy(() -> custom.add("foo"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
