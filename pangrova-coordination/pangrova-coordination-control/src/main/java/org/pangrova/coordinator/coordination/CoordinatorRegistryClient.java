package org.pangrova.coordinator.coordination;

import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.pangrova.coordination.api.enums.CoordinatorRole;
import org.pangrova.coordination.api.membership.CoordinatorSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges HA lifecycle events to the optional {@link CoordinatorRegistry}.
 * All calls are null-safe: if no registry is configured, operations are no-ops.
 */
final class CoordinatorRegistryClient {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorRegistryClient.class);

    private volatile CoordinatorRegistry registry;
    private volatile String host;
    private volatile int httpPort;

    void configure(CoordinatorRegistry configuredRegistry, String advertisedHost, int advertisedHttpPort) {
        this.registry = configuredRegistry;
        this.host = advertisedHost;
        this.httpPort = advertisedHttpPort;
    }

    void heartbeat(String coordinatorId) {
        CoordinatorRegistry reg = registry;
        if (reg == null) {
            return;
        }
        reg.heartbeat(coordinatorId);
    }

    void register(String coordinatorId, String incarnation, long epoch) {
        CoordinatorRegistry reg = registry;
        if (reg == null) {
            return;
        }
        long now = System.currentTimeMillis();
        reg.register(new CoordinatorSnapshot(
                coordinatorId, host, httpPort,
                "STANDBY", epoch, now, now, -1, incarnation));
        LOG.debug("event=coordinator_registered coordinatorId={} host={} httpPort={}",
                coordinatorId, host, httpPort);
    }

    void updateRole(String coordinatorId, CoordinatorRole newRole) {
        CoordinatorRegistry reg = registry;
        if (reg == null) {
            return;
        }
        reg.updateRole(coordinatorId, newRole);
        LOG.debug("event=coordinator_role_updated coordinatorId={} role={}", coordinatorId, newRole);
    }

    void unregister(String coordinatorId) {
        CoordinatorRegistry reg = registry;
        if (reg == null) {
            return;
        }
        reg.unregister(coordinatorId);
        LOG.debug("event=coordinator_unregistered coordinatorId={}", coordinatorId);
    }
}
