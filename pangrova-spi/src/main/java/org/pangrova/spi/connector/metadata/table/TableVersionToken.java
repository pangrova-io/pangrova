package org.pangrova.spi.connector.metadata.table;

/**
 * Opaque, comparable token for the state of a table at a point in time.
 *
 * <p>A token is only used for equality checks. It is never parsed outside the
 * connector that produced it: the canonical form is owned by the format adapter
 * and may gain components without a control-plane change.</p>
 */
public record TableVersionToken(String format, String token) {

    public TableVersionToken {
        if (format == null || format.isBlank()) {
            throw new IllegalArgumentException("format must not be blank");
        }
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("token must not be blank");
        }
    }

    /** Builds a token from ordered components; components must be individually meaningful. */
    public static TableVersionToken of(String format, String... components) {
        if (components == null || components.length == 0) {
            throw new IllegalArgumentException("version token requires at least one component");
        }
        StringBuilder token = new StringBuilder();
        for (String component : components) {
            if (component == null || component.isBlank()) {
                throw new IllegalArgumentException("version token components must not be blank");
            }
            if (token.length() > 0) {
                token.append(':');
            }
            token.append(component);
        }
        return new TableVersionToken(format, token.toString());
    }
}
