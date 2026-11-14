package org.pangrova.persistence.coordination.api.provider;

import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;

import org.pangrova.common.api.annotation.Internal;

/**
 * Coordinator membership and distributed leases for one configured backend.
 * The creating Coordinator closes the provider after both consumers drain.
 */
@Internal
public interface CoordinationProvider extends AutoCloseable {

    DistributedLease lease();

    CoordinatorRegistry registry();

    @Override
    void close();
}
