package org.pangrova.testing.testkit.security.mask;

import java.util.List;
import org.pangrova.security.api.mask.ColumnMask;
import org.pangrova.persistence.security.api.mask.EnrichedColumnMaskStore;

/**
 * No-op ColumnMaskStore that returns empty masks. Test fixture only;
 * production always uses the configured mask authority.
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
