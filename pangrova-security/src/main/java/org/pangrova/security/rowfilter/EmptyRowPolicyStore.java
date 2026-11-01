package org.pangrova.security.rowfilter;

import java.util.List;
import org.pangrova.spi.security.rowfilter.RowPolicy;
import org.pangrova.persistence.api.security.rowfilter.RowPolicyStore;

/**
 * No-op RowPolicyStore that returns empty policies. Used when RLS is not configured.
 */
public final class EmptyRowPolicyStore implements RowPolicyStore {

    @Override
    public List<RowPolicy> getPolicies(String catalog, String schema, String table) {
        return List.of();
    }

    @Override
    public void savePolicy(RowPolicy policy) {
        throw new UnsupportedOperationException("EmptyRowPolicyStore is read-only");
    }

    @Override
    public void deletePolicy(long policyId) {
        throw new UnsupportedOperationException("EmptyRowPolicyStore is read-only");
    }
}
