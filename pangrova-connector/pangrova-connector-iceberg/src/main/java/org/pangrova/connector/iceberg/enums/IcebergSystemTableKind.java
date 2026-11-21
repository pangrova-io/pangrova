package org.pangrova.connector.iceberg.enums;

import java.util.Locale;
import java.util.Optional;

/** Metadata tables supported by the Native JSON reader. */
public enum IcebergSystemTableKind {
    SNAPSHOTS("snapshots"),
    HISTORY("history"),
    REFS("refs");

    private final String suffix;

    IcebergSystemTableKind(String suffix) {
        this.suffix = suffix;
    }

    public String suffix() {
        return suffix;
    }

    /** Resolves a supported suffix without accepting a base-table fallback. */
    public static Optional<IcebergSystemTableKind> fromSuffix(String suffix) {
        if (suffix == null) {
            return Optional.empty();
        }
        String normalized = suffix.toLowerCase(Locale.ROOT);
        for (IcebergSystemTableKind kind : values()) {
            if (kind.suffix.equals(normalized)) {
                return Optional.of(kind);
            }
        }
        return Optional.empty();
    }
}
