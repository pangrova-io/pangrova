package org.pangrova.query.core.dispatch;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a DDL statement is submitted to a non-leader coordinator in
 * an HA deployment. The exception carries the leader's endpoint URL so
 * clients can redirect (HTTP 307 Temporary Redirect).
 *
 * <p>F-CHAIN-HA-06 fix: DDL must execute on the leader node to prevent
 * metadata inconsistencies in multi-coordinator deployments. Non-leader
 * coordinators reject DDL with this exception, providing the leader
 * endpoint for client-side redirect.</p>
 */
public final class DdlNotOnLeaderException extends PangrovaException {

    private final String leaderEndpoint;

    /** Reports the leader endpoint required for DDL execution. */
    public DdlNotOnLeaderException(String leaderEndpoint) {
        super(PangrovaErrorCode.COORDINATOR_DDL_NOT_ON_LEADER,
                "DDL statements must execute on the leader coordinator. "
                        + "Leader endpoint: " + (leaderEndpoint != null ? leaderEndpoint : "unknown"));
        this.leaderEndpoint = leaderEndpoint;
    }

    /**
     * Returns the leader coordinator's base URL (e.g., "http://leader-host:8080")
     * for client redirect. May be null if the leader is unknown (no active leader).
     */
    public String getLeaderEndpoint() {
        return leaderEndpoint;
    }
}
