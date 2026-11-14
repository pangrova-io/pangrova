package org.pangrova.persistence.coordination.postgresql.provider;

import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.persistence.coordination.api.provider.CoordinationProvider;
import org.pangrova.persistence.coordination.api.provider.CoordinationProviderFactory;
import org.pangrova.persistence.coordination.postgresql.lease.JdbcDistributedLease;
import org.pangrova.persistence.coordination.postgresql.membership.JdbcCoordinatorRegistry;

import javax.sql.DataSource;
import java.util.function.Function;

public final class JdbcCoordinationProviderFactory implements CoordinationProviderFactory {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(JdbcCoordinationProviderFactory.class);

    @Override
    public int apiVersion() {
        return API_VERSION;
    }

    @Override
    public String id() {
        return "jdbc";
    }

    @Override
    public CoordinationProvider open(DataSource metadata, Function<String, String> setting) {
        LOG.warn("event=coordination_spof_posture backend=jdbc "
                + "message=\"multi-coordinator lease, registry and journal require a highly available database\"");
        return new Provider(new JdbcDistributedLease(metadata), new JdbcCoordinatorRegistry(metadata));
    }

    private record Provider(DistributedLease lease, CoordinatorRegistry registry) implements CoordinationProvider {
        @Override
        public void close() {
            // Both repositories borrow the pool owned by the server.
        }
    }
}
