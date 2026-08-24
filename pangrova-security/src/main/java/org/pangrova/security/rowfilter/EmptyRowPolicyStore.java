package org.pangrova.security.rowfilter;

import java.util.List;

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
