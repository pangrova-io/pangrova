package org.pangrova.coordinator.coordination;

import org.pangrova.spi.coordination.CoordinatorRegistry;
import org.pangrova.spi.coordination.CoordinatorRole;
import org.pangrova.spi.coordination.CoordinatorSnapshot;
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
        try {
            reg.heartbeat(coordinatorId);
        } catch (Exception e) {
            LOG.warn("event=coordinator_heartbeat_failed coordinatorId={} error={}",
                    coordinatorId, e.getMessage());
        }
    }

    void register(String coordinatorId, long epoch) {
        CoordinatorRegistry reg = registry;
        if (reg == null) {
            return;
        }
        try {
            long now = System.currentTimeMillis();
            reg.register(new CoordinatorSnapshot(
                    coordinatorId, host, httpPort,
                    "STANDBY", epoch, now, now));
            LOG.debug("event=coordinator_registered coordinatorId={} host={} httpPort={}",
                    coordinatorId, host, httpPort);
        } catch (Exception e) {
            LOG.error("event=coordinator_register_failed coordinatorId={}", coordinatorId, e);
        }
    }

    void updateRole(String coordinatorId, CoordinatorRole newRole) {
        CoordinatorRegistry reg = registry;
        if (reg == null) {
            return;
        }
        try {
            reg.updateRole(coordinatorId, newRole);
            LOG.debug("event=coordinator_role_updated coordinatorId={} role={}", coordinatorId, newRole);
        } catch (Exception e) {
            LOG.error("event=coordinator_role_update_failed coordinatorId={} role={}",
                    coordinatorId, newRole, e);
        }
    }

    void unregister(String coordinatorId) {
        CoordinatorRegistry reg = registry;
        if (reg == null) {
            return;
        }
        try {
            reg.unregister(coordinatorId);
            LOG.debug("event=coordinator_unregistered coordinatorId={}", coordinatorId);
        } catch (Exception e) {
            LOG.error("event=coordinator_unregister_failed coordinatorId={}", coordinatorId, e);
        }
    }
}
