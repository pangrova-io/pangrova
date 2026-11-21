package org.pangrova.persistence.security.api.rowfilter;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.security.api.rowfilter.RowPolicy;

import java.util.List;

/**
 * Storage abstraction for row-level security policies.
 */
@Experimental
public interface RowPolicyStore {

    List<RowPolicy> getPolicies(String catalog, String schema, String table);

    void savePolicy(RowPolicy policy);

    void deletePolicy(long policyId);
}
