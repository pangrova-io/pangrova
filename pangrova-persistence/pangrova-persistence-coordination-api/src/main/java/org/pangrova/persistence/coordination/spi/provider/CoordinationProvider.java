package org.pangrova.persistence.coordination.spi.provider;

import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;

import org.pangrova.common.api.annotation.Internal;

/**
 * Optional quorum-backed coordination backend: distributed lease and coordinator
 * registry sharing one connection and one close hook. Implemented outside the
 * Coordinator artifact so the default relational backend carries no quorum SDK.
 */
@Internal
public interface CoordinationProvider extends AutoCloseable {

    DistributedLease lease();

    CoordinatorRegistry registry();

    @Override
    void close();
}
