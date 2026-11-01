package org.pangrova.persistence.api.security.rowfilter;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.spi.security.rowfilter.RowPolicy;

import java.util.List;
import java.util.UUID;

/** Canonical row-policy persistence with stable table identity independent of diagnostic names. */
@Experimental
public interface IdentityRowPolicyStore extends RowPolicyStore {
    /** Returns enabled policies for the captured identity, including after a table rename. */
    List<RowPolicy> getPolicies(UUID tableId);
}
