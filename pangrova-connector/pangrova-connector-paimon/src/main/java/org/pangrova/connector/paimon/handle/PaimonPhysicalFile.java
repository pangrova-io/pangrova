package org.pangrova.connector.paimon.handle;

import java.util.Locale;

/** Immutable physical file identity read from the Paimon snapshot's DataFileMeta. */
public record PaimonPhysicalFile(String path, String format, long fileSize, long rowCount, long schemaId) {
    public PaimonPhysicalFile {
        requireIdentity(path, format);
        format = format.toLowerCase(Locale.ROOT);
        requireFormat(format);
        if (fileSize <= 0 || rowCount < 0 || schemaId < 0) {
            throw new IllegalArgumentException("Invalid Paimon physical file size, row count or schema ID");
        }
    }

    private static void requireIdentity(String path, String format) {
        if (path == null || path.isBlank() || format == null || format.isBlank()) {
            throw new IllegalArgumentException("Paimon physical file requires an exact path and format");
        }
    }

    private static void requireFormat(String format) {
        if (!format.equals("parquet") && !format.equals("orc") && !format.equals("lance")) {
            throw new IllegalArgumentException("Unsupported Paimon physical file format: " + format);
        }
    }
}
