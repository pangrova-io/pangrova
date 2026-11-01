package org.pangrova.coordinator.bootstrap;

import org.pangrova.persistence.jdbc.coordination.JdbcDistributedLease;
import org.pangrova.persistence.api.coordination.CoordinationProvider;
import org.pangrova.persistence.api.coordination.DistributedLease;
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
        var provider = loadQuorumProvider(connectString, basePath);
        return new Binding(provider.lease(), provider);
    }

    private static CoordinationProvider loadQuorumProvider(String connectString, String basePath) {
        try {
            Class<?> type = Class.forName("org.pangrova.coordination.zookeeper.ZooKeeperLeaseProvider",
                    true, CoordinatorLeaseBootstrap.class.getClassLoader());
            return (CoordinationProvider) type.getConstructor(String.class, String.class)
                    .newInstance(connectString, basePath);
        } catch (ClassNotFoundException missing) {
            throw new IllegalStateException("coordination lease backend=zookeeper requires "
                    + "pangrova-coordination-zookeeper on the runtime classpath", missing);
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Failed to create the ZooKeeper coordination provider", failure);
        }
    }

    record Binding(DistributedLease lease, CoordinationProvider quorumProvider) {}
}
