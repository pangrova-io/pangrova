package org.pangrova.connector.paimon.handle;

/** Exact shared-object byte domain supplied by Paimon's DeletionFile, not a raw bitmap file. */
public record PaimonDeletionVector(String path, long offset, long length, Long cardinality, long physicalRows) {
    public PaimonDeletionVector {
        if (path == null || path.isBlank() || offset < 0 || length <= 0 || physicalRows < 0) {
            throw new IllegalArgumentException("Invalid Paimon deletion vector range");
        }
        requireCardinality(cardinality, physicalRows);
        if (offset > Long.MAX_VALUE - length) {
            throw new ArithmeticException("long overflow");
        }
    }

    private static void requireCardinality(Long cardinality, long physicalRows) {
        if (cardinality != null && (cardinality < 0 || cardinality > physicalRows)) {
            throw new IllegalArgumentException("Invalid Paimon deletion vector cardinality");
        }
    }
}
