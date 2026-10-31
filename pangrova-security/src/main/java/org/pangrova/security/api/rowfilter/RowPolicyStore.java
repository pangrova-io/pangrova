package org.pangrova.security.api.rowfilter;

import java.util.List;

/**
 * Storage abstraction for row-level security policies.
 */
public interface RowPolicyStore {

    List<RowPolicy> getPolicies(String catalog, String schema, String table);

    void savePolicy(RowPolicy policy);

    void deletePolicy(long policyId);
}
