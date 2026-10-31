package org.pangrova.security.api.rowfilter;

import java.util.List;
import java.util.UUID;

/** Canonical row-policy persistence with stable table identity independent of diagnostic names. */
public interface IdentityRowPolicyStore extends RowPolicyStore {
    /** Returns enabled policies for the captured identity, including after a table rename. */
    List<RowPolicy> getPolicies(UUID tableId);
}
