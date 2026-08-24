package org.pangrova.operations.upgrade;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link VersionCompatibilityChecker}, {@link SemanticVersion}, and {@link VersionRange}.
 */
class VersionCompatibilityCheckerTest {

    // --- VersionCompatibilityChecker exact-match mode ---

    @Test
    void exactMatch_compatibleVersions() {
        var checker = new VersionCompatibilityChecker(
                Map.of("1.0.0", 1, "1.1.0", 2, "2.0.0", 3));
        assertThat(checker.isCompatible("1.0.0", "1.1.0")).isTrue();
        assertThat(checker.isCompatible("1.1.0", "2.0.0")).isTrue();
    }

    @Test
    void exactMatch_incompatibleVersions() {
        var checker = new VersionCompatibilityChecker(
                Map.of("1.0.0", 1, "2.0.0", 3));
        // Protocol diff is 2 — not compatible
        assertThat(checker.isCompatible("1.0.0", "2.0.0")).isFalse();
    }

    @Test
    void exactMatch_unknownVersionReturnsIncompatible() {
        var checker = new VersionCompatibilityChecker(
                Map.of("1.0.0", 1, "1.1.0", 2));
        // "1.0.5" is not a registered key
        assertThat(checker.isCompatible("1.0.5", "1.1.0")).isFalse();
    }

    @Test
    void exactMatch_sameVersionIsCompatible() {
        var checker = new VersionCompatibilityChecker(
                Map.of("1.0.0", 1));
        assertThat(checker.isCompatible("1.0.0", "1.0.0")).isTrue();
    }

    // --- Native compatibility ---

    @Test
    void nativeCompatibility_sameMajorIsCompatible() {
        var checker = new VersionCompatibilityChecker(Map.of());
        assertThat(checker.isNativeCompatible("1.2.3", "1.5.0")).isTrue();
    }

    @Test
    void nativeCompatibility_differentMajorIsIncompatible() {
        var checker = new VersionCompatibilityChecker(Map.of());
        assertThat(checker.isNativeCompatible("1.2.3", "2.0.0")).isFalse();
    }

    // --- SemanticVersion parsing ---

    @Test
    void semanticVersion_parsesFullVersion() {
        SemanticVersion v = SemanticVersion.parse("1.2.3");
        assertThat(v.major()).isEqualTo(1);
        assertThat(v.minor()).isEqualTo(2);
        assertThat(v.patch()).isEqualTo(3);
        assertThat(v.preRelease()).isNull();
    }

    @Test
    void semanticVersion_parsesPreRelease() {
        SemanticVersion v = SemanticVersion.parse("2.0.0-beta");
        assertThat(v.major()).isEqualTo(2);
        assertThat(v.minor()).isEqualTo(0);
        assertThat(v.patch()).isEqualTo(0);
        assertThat(v.preRelease()).isEqualTo("beta");
    }

    @Test
    void semanticVersion_parsesPartialVersion() {
        SemanticVersion v = SemanticVersion.parse("3.1");
        assertThat(v.major()).isEqualTo(3);
        assertThat(v.minor()).isEqualTo(1);
        assertThat(v.patch()).isEqualTo(0);
    }

    @Test
    void semanticVersion_ordering() {
        SemanticVersion v1 = SemanticVersion.parse("1.0.0");
        SemanticVersion v2 = SemanticVersion.parse("1.0.1");
        SemanticVersion v3 = SemanticVersion.parse("1.1.0");
        SemanticVersion v4 = SemanticVersion.parse("2.0.0");

        assertThat(v1).isLessThan(v2);
        assertThat(v2).isLessThan(v3);
        assertThat(v3).isLessThan(v4);
    }

    @Test
    void semanticVersion_preReleaseHasLowerPrecedence() {
        SemanticVersion release = SemanticVersion.parse("1.0.0");
        SemanticVersion preRelease = SemanticVersion.parse("1.0.0-alpha");
        assertThat(preRelease).isLessThan(release);
    }

    @Test
    void semanticVersion_invalidThrows() {
        assertThatThrownBy(() -> SemanticVersion.parse("not.a.version"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --- VersionRange parsing ---

    @Test
    void versionRange_greaterThanOrEqual() {
        VersionRange range = VersionRange.parse(">=1.5.0");
        assertThat(range.satisfiedBy("1.5.0")).isTrue();
        assertThat(range.satisfiedBy("1.6.0")).isTrue();
        assertThat(range.satisfiedBy("2.0.0")).isTrue();
        assertThat(range.satisfiedBy("1.4.9")).isFalse();
    }

    @Test
    void versionRange_lessThan() {
        VersionRange range = VersionRange.parse("<2.0.0");
        assertThat(range.satisfiedBy("1.9.9")).isTrue();
        assertThat(range.satisfiedBy("1.0.0")).isTrue();
        assertThat(range.satisfiedBy("2.0.0")).isFalse();
        assertThat(range.satisfiedBy("2.0.1")).isFalse();
    }

    @Test
    void versionRange_conjunction() {
        VersionRange range = VersionRange.parse(">=1.2.0 <2.0.0");
        assertThat(range.satisfiedBy("1.2.0")).isTrue();
        assertThat(range.satisfiedBy("1.5.3")).isTrue();
        assertThat(range.satisfiedBy("1.99.99")).isTrue();
        assertThat(range.satisfiedBy("1.1.9")).isFalse();
        assertThat(range.satisfiedBy("2.0.0")).isFalse();
    }

    @Test
    void versionRange_caretRange() {
        // ^1.2.3 means >=1.2.3 <2.0.0
        VersionRange range = VersionRange.parse("^1.2.3");
        assertThat(range.satisfiedBy("1.2.3")).isTrue();
        assertThat(range.satisfiedBy("1.9.0")).isTrue();
        assertThat(range.satisfiedBy("1.2.2")).isFalse();
        assertThat(range.satisfiedBy("2.0.0")).isFalse();
    }

    @Test
    void versionRange_tildeRange() {
        // ~1.2.3 means >=1.2.3 <1.3.0
        VersionRange range = VersionRange.parse("~1.2.3");
        assertThat(range.satisfiedBy("1.2.3")).isTrue();
        assertThat(range.satisfiedBy("1.2.9")).isTrue();
        assertThat(range.satisfiedBy("1.3.0")).isFalse();
        assertThat(range.satisfiedBy("1.2.2")).isFalse();
    }

    @Test
    void versionRange_exactVersion() {
        VersionRange range = VersionRange.parse("1.5.0");
        assertThat(range.satisfiedBy("1.5.0")).isTrue();
        assertThat(range.satisfiedBy("1.5.1")).isFalse();
        assertThat(range.satisfiedBy("1.4.9")).isFalse();
    }
}
