package org.pangrova.metadata.sync;

import java.util.List;

/** Summary of one catalog's version-observation pass. */
public record TableVersionObservationReport(
        String catalogName,
        int checked,
        int versionChanges,
        int identityChanges,
        int missing,
        int unsupported,
        int ignored,
        List<String> errors) {

    public TableVersionObservationReport {
        errors = errors == null ? List.of() : List.copyOf(errors);
    }

    public static TableVersionObservationReport empty(String catalogName) {
        return new TableVersionObservationReport(
                catalogName, 0, 0, 0, 0, 0, 0, List.of());
    }

    /** The catalog was unreachable; no table was reclassified. */
    public static TableVersionObservationReport unavailable(String catalogName, String error) {
        return new TableVersionObservationReport(
                catalogName, 0, 0, 0, 0, 0, 0, List.of(error));
    }

    public boolean hasFailures() {
        return !errors.isEmpty();
    }
}
