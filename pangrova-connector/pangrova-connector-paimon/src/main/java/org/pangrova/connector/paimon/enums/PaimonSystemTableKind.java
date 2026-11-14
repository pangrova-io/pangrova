package org.pangrova.connector.paimon.enums;

/** Native metadata readers supported by this handle. */
public enum PaimonSystemTableKind {
    SNAPSHOTS("snapshots"), TAGS("tags"), OPTIONS("options"), SCHEMAS("schemas");

    private final String suffix;

    PaimonSystemTableKind(String suffix) { this.suffix = suffix; }

    public String suffix() { return suffix; }
}
