package org.pangrova.materialization.strategy;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.pangrova.materialization.domain.MaterializationException;
import org.pangrova.materialization.domain.PhysicalTableRef;

/** Canonical SQL and fingerprint helpers shared by built-in refresh strategies. */
public final class RefreshSql {

    private RefreshSql() {
    }

    /**
     * Returns the definition as an embeddable query.
     *
     * <p>Trailing statement terminators are removed because the definition is
     * composed into a larger UNION ALL statement; anything else is preserved
     * exactly as submitted.
     */
    public static String definitionQuery(String sql) {
        if (sql == null) {
            throw MaterializationException.invalidDefinition("materialization definition is missing");
        }
        String query = sql.strip();
        while (query.endsWith(";")) {
            query = query.substring(0, query.length() - 1).stripTrailing();
        }
        if (query.isEmpty()) {
            throw MaterializationException.invalidDefinition("materialization definition is empty");
        }
        return query;
    }

    /** Quotes one identifier component for a canonical statement. */
    public static String quote(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            throw MaterializationException.invalidDefinition(
                    "physical table identifiers must not be blank");
        }
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    /** Quotes a physical table for use in a FROM clause. */
    public static String qualified(PhysicalTableRef table) {
        return quote(table.catalog()) + '.' + quote(table.schema()) + '.' + quote(table.table());
    }

    /** Returns the lowercase hexadecimal SHA-256 digest of the UTF-8 value. */
    public static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", failure);
        }
    }
}
