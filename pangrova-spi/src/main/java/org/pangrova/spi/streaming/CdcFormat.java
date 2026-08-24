package org.pangrova.spi.streaming;

/**
 * Supported CDC envelope formats for native transform path.
 *
 * <p>Each format defines a different wire protocol for change data capture events.
 * The native worker uses this to select the appropriate envelope parser when
 * processing CDC batches in Arrow columnar memory.</p>
 */
public enum CdcFormat {

    /** Debezium CDC format (default). */
    DEBEZIUM("debezium"),

    /** Alibaba Canal CDC format. */
    CANAL("canal"),

    /** Zendesk Maxwell CDC format. */
    MAXWELL("maxwell");

    private final String formatName;

    CdcFormat(String formatName) {
        this.formatName = formatName;
    }

    /** Returns the wire-protocol name used in WriteDescriptor properties. */
    public String formatName() {
        return formatName;
    }

    /**
     * Resolves a CDC format from a string value.
     *
     * @param value format name (case-insensitive)
     * @return matching CdcFormat
     * @throws IllegalArgumentException if the format is not recognized
     */
    public static CdcFormat fromString(String value) {
        if (value == null || value.isBlank()) {
            return DEBEZIUM;
        }
        String normalized = value.strip().toLowerCase();
        for (CdcFormat format : values()) {
            if (format.formatName.equals(normalized)) {
                return format;
            }
        }
        throw new IllegalArgumentException("Unsupported CDC format: " + value
                + ". Supported formats: debezium, canal, maxwell");
    }

    /** WriteDescriptor properties key for CDC format. */
    public static final String PROPERTY_KEY = "cdc.format";
}
