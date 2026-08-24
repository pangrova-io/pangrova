package org.pangrova.operations.upgrade;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A parsed semantic version (major.minor.patch) with optional pre-release suffix.
 * Supports comparison and ordering per the semver 2.0 spec (pre-release ordering
 * is simplified — only presence/absence matters, not full precedence).
 */
public final class SemanticVersion implements Comparable<SemanticVersion> {

    private static final Pattern SEMVER_PATTERN =
            Pattern.compile("^(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?(?:-(.+))?$");

    private final int major;
    private final int minor;
    private final int patch;
    private final String preRelease; // nullable

    /** Creates a version from explicit components; preRelease may be null. */
    public SemanticVersion(int major, int minor, int patch, String preRelease) {
        if (major < 0 || minor < 0 || patch < 0) {
            throw new IllegalArgumentException("Version components must be non-negative");
        }
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.preRelease = preRelease;
    }

    public SemanticVersion(int major, int minor, int patch) {
        this(major, minor, patch, null);
    }

    /**
     * Parses a version string like "1.2.3", "1.2", "1", or "1.2.3-beta".
     */
    public static SemanticVersion parse(String version) {
        Objects.requireNonNull(version, "version");
        String trimmed = version.trim();
        Matcher matcher = SEMVER_PATTERN.matcher(trimmed);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid semantic version: " + version);
        }
        int major = Integer.parseInt(matcher.group(1));
        int minor = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 0;
        int patch = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
        String preRelease = matcher.group(4);
        return new SemanticVersion(major, minor, patch, preRelease);
    }

    public int major() {
        return major;
    }

    public int minor() {
        return minor;
    }

    public int patch() {
        return patch;
    }

    public String preRelease() {
        return preRelease;
    }

    public boolean isPreRelease() {
        return preRelease != null;
    }

    @Override
    public int compareTo(SemanticVersion other) {
        int cmp = Integer.compare(this.major, other.major);
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(this.minor, other.minor);
        if (cmp != 0) {
            return cmp;
        }
        cmp = Integer.compare(this.patch, other.patch);
        if (cmp != 0) {
            return cmp;
        }
        // Pre-release versions have lower precedence than release
        if (this.preRelease == null && other.preRelease == null) {
            return 0;
        }
        if (this.preRelease == null) {
            return 1; // release > pre-release
        }
        if (other.preRelease == null) {
            return -1;
        }
        return this.preRelease.compareTo(other.preRelease);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SemanticVersion that)) {
            return false;
        }
        return major == that.major && minor == that.minor && patch == that.patch
                && Objects.equals(preRelease, that.preRelease);
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, patch, preRelease);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(patch);
        if (preRelease != null) {
            sb.append('-').append(preRelease);
        }
        return sb.toString();
    }
}
