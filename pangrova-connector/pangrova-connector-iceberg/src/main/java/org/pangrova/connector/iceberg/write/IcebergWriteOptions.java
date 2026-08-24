package org.pangrova.connector.iceberg.write;

import java.util.Map;

/**
 * Tunable options for the Iceberg write path.
 *
 * <p>These options live alongside {@link IcebergWriteContext} and travel with
 * a single insert/overwrite operation. They are typically derived from
 * {@code ConnectorSession} properties at sink-creation time.</p>
 *
 * @param strictColumnMapping when {@code true} (default), the page sink validates
 *                            that the source page columns exactly match the target
 *                            Iceberg schema columns at the first {@code appendPage}
 *                            call:
 *                            <ul>
 *                              <li>extra source columns not in the target schema → error;</li>
 *                              <li>missing required (NOT NULL) target columns → error;</li>
 *                              <li>missing nullable target columns → warn once and fill NULL.</li>
 *                            </ul>
 *                            When {@code false}, extra source columns are warned and
 *                            ignored instead of failing. Missing required columns are
 *                            <em>always</em> rejected because the underlying writer
 *                            cannot serialize NULL into a required field.
 */
public record IcebergWriteOptions(boolean strictColumnMapping) {

    /** Session property key controlling {@link #strictColumnMapping}. */
    public static final String PROP_STRICT_COLUMN_MAPPING = "iceberg.write.strict-column-mapping";

    /** Default options: strict column mapping enabled (fails fast on schema mismatch). */
    public static final IcebergWriteOptions DEFAULT = new IcebergWriteOptions(true);

    /**
     * Builds options from a session-properties map. Unknown keys are ignored.
     * The strict flag accepts {@code "true"} / {@code "false"} (case-insensitive);
     * any other value falls back to the default ({@code true}).
     */
    public static IcebergWriteOptions fromProperties(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return DEFAULT;
        }
        String raw = properties.get(PROP_STRICT_COLUMN_MAPPING);
        if (raw == null) {
            return DEFAULT;
        }
        boolean strict = !"false".equalsIgnoreCase(raw.trim());
        return new IcebergWriteOptions(strict);
    }
}
