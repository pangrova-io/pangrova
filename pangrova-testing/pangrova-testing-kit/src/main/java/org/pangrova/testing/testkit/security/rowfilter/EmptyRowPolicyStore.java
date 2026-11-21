package org.pangrova.testing.testkit.security.rowfilter;

import java.util.List;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.api.rowfilter.RowPolicyStore;

/**
 * No-op RowPolicyStore that returns empty policies. Test fixture only;
 * production always uses the configured policy authority.
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
