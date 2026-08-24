package org.pangrova.connector.iceberg.lance;

/**
 * Declares the Lance file format for Iceberg table metadata registration.
 *
 * <p>This is a lightweight metadata-only registration. Lance files are read
 * entirely in the C++ native layer (LanceBatchReader via Lance C FFI).
 * Schema, statistics, and partition information come from Iceberg manifests,
 * not from opening .lance files on the Java side.</p>
 *
 * <p>Enables {@code 'write.format.default' = 'lance'} table property on
 * Iceberg tables, allowing the split manager to recognize .lance data files
 * and dispatch them to the native Lance reader.</p>
 */
public final class IcebergLanceFormatModel {

    private static final String FORMAT_NAME = "lance";
    private static final String FILE_EXTENSION = ".lance";

    /**
     * Returns the canonical format name used in Iceberg table properties
     * and manifest entries.
     */
    public String formatName() {
        return FORMAT_NAME;
    }

    /**
     * Returns the file extension for Lance data files.
     */
    public String fileExtension() {
        return FILE_EXTENSION;
    }

    @Override
    public String toString() {
        return "IcebergLanceFormatModel[name=" + FORMAT_NAME + ", ext=" + FILE_EXTENSION + "]";
    }
}
