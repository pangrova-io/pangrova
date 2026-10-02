package org.pangrova.coordinator.bootstrap;

import org.pangrova.metadata.coordination.JdbcDistributedLease;
import org.pangrova.metadata.coordination.ZooKeeperLeaseProvider;
import org.pangrova.spi.coordination.DistributedLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Locale;

/** Owns exact lease backend selection; lifecycle ownership stays with the Coordinator bootstrap. */
final class CoordinatorLeaseBootstrap {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLeaseBootstrap.class);

    private CoordinatorLeaseBootstrap() {}

    static Binding resolve(DataSource dataSource) {
        String backend = System.getProperty("pangrova.coordination.lease.backend", "jdbc")
                .trim().toLowerCase(Locale.ROOT);
        return switch (backend) {
            case "jdbc" -> jdbc(dataSource);
            case "zookeeper", "zk" -> quorum();
            default -> throw new IllegalArgumentException("Unknown coordination lease backend: " + backend);
        };
    }

    private static Binding jdbc(DataSource dataSource) {
        LOG.warn("event=coordination_spof_posture backend=jdbc "
                + "message=\"lease, registry and journal require the relational metadata backend; "
                + "deploy a highly available database for multi-coordinator operation\"");
        LOG.info("event=distributed_lease_configured backend=jdbc");
        return new Binding(new JdbcDistributedLease(dataSource), null);
    }

    private static Binding quorum() {
        String connectString = System.getProperty("pangrova.coordination.lease.zookeeper.connectString");
        if (connectString == null || connectString.isBlank()) {
            throw new IllegalStateException("coordination lease backend=zookeeper requires "
                    + "pangrova.coordination.lease.zookeeper.connectString");
        }
        String basePath = System.getProperty("pangrova.coordination.lease.zookeeper.basePath", "/pangrova/leases");
        var provider = new ZooKeeperLeaseProvider(connectString, basePath);
        return new Binding(provider.lease(), provider);
    }

    record Binding(DistributedLease lease, ZooKeeperLeaseProvider quorumProvider) {}
}
