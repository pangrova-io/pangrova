package org.pangrova.security.mask;

import java.util.List;

/**
 * No-op ColumnMaskStore that returns empty masks. Used when column masking is not configured.
 */
public final class EmptyColumnMaskStore implements EnrichedColumnMaskStore {

    @Override
    public List<ColumnMask> getTableMasks(String catalog, String schema, String table) {
        return List.of();
    }

    @Override
    public List<ColumnMask> getColumnMasks(String catalog, String schema, String table, String column) {
        return List.of();
    }

    @Override
    public void saveColumnMask(ColumnMask mask) {
        throw new UnsupportedOperationException("EmptyColumnMaskStore is read-only");
    }

    @Override
    public void deleteMask(long maskId) {
        throw new UnsupportedOperationException("EmptyColumnMaskStore is read-only");
    }
}
