package org.pangrova.coordinator.dynamictable.detect;

/**
 * Thrown when a CDC event's schema version does not match the expected
 * version recorded at Dynamic Table creation time.
 *
 * <p>Carries context about the mismatch so the caller can log a
 * meaningful diagnostic and transition the DT to SUSPENDED state.</p>
 */
public final class SchemaEvolutionException extends RuntimeException {

    private final int expectedVersion;
    private final int actualVersion;
    private final String dtName;

    public SchemaEvolutionException(int expectedVersion, int actualVersion, String dtName) {
        super(buildMessage(expectedVersion, actualVersion, dtName));
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
        this.dtName = dtName;
    }

    public int expectedVersion() { return expectedVersion; }

    public int actualVersion() { return actualVersion; }

    public String dtName() { return dtName; }

    private static String buildMessage(int expected, int actual, String dtName) {
        return "Schema evolution detected for dtName=" + dtName
                + " expectedVersion=" + expected
                + " actualVersion=" + actual;
    }
}
