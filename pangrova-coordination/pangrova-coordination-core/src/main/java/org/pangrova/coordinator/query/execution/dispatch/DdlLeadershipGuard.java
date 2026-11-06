package org.pangrova.coordinator.query.execution.dispatch;

import java.util.Objects;
import java.util.Optional;

import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.query.core.statement.StatementClassifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.persistence.coordination.api.membership.CoordinatorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guards DDL execution on non-leader coordinators in HA deployments.
 *
 * <p>This guard is <strong>optional and disabled by default</strong>. It is only
 * instantiated when {@code coordinator.ha.ddl-leader-routing=true} is explicitly
 * configured. When disabled (the default), DDL executes on any coordinator —
 * metadata consistency is guaranteed by the connector's own concurrency control
 * (Iceberg optimistic versioning, Paimon catalog lock) and the cross-node
 * cache invalidation propagation via {@code invalidation_log}.</p>
 *
 * <p>When enabled, DDL statements (CREATE/ALTER/DROP TABLE/SCHEMA/CATALOG,
 * index DDL, stream DDL, GRANT/REVOKE) are rejected on non-leader nodes with a
 * {@link DdlNotOnLeaderException} containing the leader's endpoint URL
 * for client-side redirect (HTTP 307).</p>
 *
 * <p>Non-DDL statements (SELECT, DML, SHOW/DESCRIBE, maintenance) are
 * always allowed on any coordinator node regardless of this guard.</p>
 *
 * <p>Thread safety: all fields are immutable or volatile. The HA service
 * and registry are thread-safe by contract.</p>
 *
 * <p>Leader routing is selected explicitly by process configuration.</p>
 */
public final class DdlLeadershipGuard {

    private static final Logger LOG = LoggerFactory.getLogger(DdlLeadershipGuard.class);

    private final HighAvailabilityCoordinationService haService;
    private final String localCoordinatorId;

    public DdlLeadershipGuard(HighAvailabilityCoordinationService haService,
                              String localCoordinatorId) {
        this.haService = Objects.requireNonNull(haService, "haService must not be null");
        this.localCoordinatorId = Objects.requireNonNull(localCoordinatorId,
                "localCoordinatorId must not be null");
    }

    /**
     * Checks if the given statement requires leader routing and, if so,
     * verifies the local node is the leader. Throws {@link DdlNotOnLeaderException}
     * if DDL is submitted to a non-leader.
     *
     * @param stmt the parsed statement to check
     * @throws DdlNotOnLeaderException if the statement is DDL and this node is not the leader
     */
    public void requireLeaderForDdl(Statement stmt) {
        if (!StatementClassifier.requiresLeaderRouting(stmt)) {
            return;
        }
        if (haService.isLeader(localCoordinatorId)) {
            return;
        }
        // This node is not the leader — resolve the leader endpoint for redirect
        Optional<String> leaderEndpoint = resolveLeaderEndpoint();
        LOG.warn("event=ddl_rejected_not_leader localNode={} leaderEndpoint={} statementType={}",
                localCoordinatorId, leaderEndpoint.orElse("unknown"), stmt.getClass().getSimpleName());
        throw new DdlNotOnLeaderException(leaderEndpoint.orElse(null));
    }

    private Optional<String> resolveLeaderEndpoint() {
        CoordinatorRegistry registry = haService.coordinatorRegistry();
        if (registry == null) {
            return Optional.empty();
        }
        return registry.getLeader().map(snapshot ->
                "http://" + snapshot.host() + ":" + snapshot.httpPort()
        );
    }
}
