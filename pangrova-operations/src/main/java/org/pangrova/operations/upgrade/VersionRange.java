package org.pangrova.operations.upgrade;

import org.pangrova.operations.enums.VersionComparison;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A semver range constraint that can test whether a given version satisfies it.
 *
 * <p>Supported formats:
 * <ul>
 *   <li>{@code >=1.2.0 <2.0.0} — conjunction of comparator constraints (space-separated)</li>
 *   <li>{@code >=1.2.0} — single lower bound</li>
 *   <li>{@code <2.0.0} — single upper bound</li>
 *   <li>{@code 1.2.0} — exact match (sugar for >=1.2.0 <1.2.1)</li>
 *   <li>{@code ^1.2.0} — caret range: >=1.2.0 <2.0.0 (compatible with major)</li>
 *   <li>{@code ~1.2.0} — tilde range: >=1.2.0 <1.3.0 (compatible with minor)</li>
 * </ul>
 */
public final class VersionRange {

    private static final Pattern COMPARATOR_PATTERN =
            Pattern.compile("(>=|<=|>|<|=|\\^|~)?\\s*(\\d+(?:\\.\\d+)?(?:\\.\\d+)?(?:-.+)?)");

    private final List<Constraint> constraints;
    private final String original;

    private VersionRange(List<Constraint> constraints, String original) {
        this.constraints = constraints;
        this.original = original;
    }

    /**
     * Parses a semver range expression.
     *
     * @param rangeExpr the range expression string
     * @return parsed VersionRange
     * @throws IllegalArgumentException if the expression cannot be parsed
     */
    public static VersionRange parse(String rangeExpr) {
        Objects.requireNonNull(rangeExpr, "rangeExpr");
        String trimmed = rangeExpr.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Empty version range expression");
        }

        List<Constraint> constraints = new ArrayList<>();
        Matcher matcher = COMPARATOR_PATTERN.matcher(trimmed);
        boolean found = false;

        while (matcher.find()) {
            found = true;
            addConstraints(matcher.group(1), SemanticVersion.parse(matcher.group(2)), constraints);
        }

        if (!found) {
            throw new IllegalArgumentException("Cannot parse version range: " + rangeExpr);
        }
        return new VersionRange(constraints, trimmed);
    }

    /**
     * Expands a single comparator (operator + version) into primitive constraints.
     */
    private static void addConstraints(String op, SemanticVersion version, List<Constraint> constraints) {
        if (op == null || op.isEmpty() || op.equals("=")) {
            // Exact match — only this specific version
            constraints.add(new Constraint(VersionComparison.GTE, version));
            constraints.add(new Constraint(VersionComparison.LTE, version));
        } else if (op.equals("^")) {
            // Caret range: >=version <next-major
            constraints.add(new Constraint(VersionComparison.GTE, version));
            constraints.add(new Constraint(VersionComparison.LT,
                    new SemanticVersion(version.major() + 1, 0, 0)));
        } else if (op.equals("~")) {
            // Tilde range: >=version <next-minor
            constraints.add(new Constraint(VersionComparison.GTE, version));
            constraints.add(new Constraint(VersionComparison.LT,
                    new SemanticVersion(version.major(), version.minor() + 1, 0)));
        } else {
            VersionComparison operator = VersionComparison.fromSymbol(op);
            constraints.add(new Constraint(operator, version));
        }
    }

    /**
     * Tests whether the given version satisfies all constraints in this range.
     */
    public boolean satisfiedBy(SemanticVersion version) {
        Objects.requireNonNull(version, "version");
        for (Constraint constraint : constraints) {
            if (!constraint.test(version)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Tests whether the given version string satisfies this range.
     */
    public boolean satisfiedBy(String version) {
        return satisfiedBy(SemanticVersion.parse(version));
    }

    @Override
    public String toString() {
        return original;
    }


    private record Constraint(VersionComparison operator, SemanticVersion bound) {
        boolean test(SemanticVersion version) {
            int cmp = version.compareTo(bound);
            return switch (operator) {
                case GTE -> cmp >= 0;
                case LTE -> cmp <= 0;
                case GT -> cmp > 0;
                case LT -> cmp < 0;
                case EQ -> cmp == 0;
            };
        }
    }
}
