package org.pangrova.security.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Health status of an authenticator backend.
 *
 * <ul>
 *   <li>{@link #HEALTHY} — backend is reachable and responding normally</li>
 *   <li>{@link #DEGRADED} — backend experienced recent failures but is not fully down</li>
 *   <li>{@link #UNHEALTHY} — backend is unreachable or consistently failing</li>
 * </ul>
 */
@Experimental
public enum AuthenticatorHealthStatus {
    HEALTHY,
    DEGRADED,
    UNHEALTHY
}
