package org.pangrova.spi.security;

import org.pangrova.spi.annotation.Experimental;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SPI for delegating authorization decisions to an external policy engine
 * such as Apache Ranger or Open Policy Agent (OPA).
 *
 * <p>When configured, the coordinator's access control chain consults this evaluator
 * after (or instead of) the built-in RBAC grant store. This enables centralized,
 * organization-wide policy management that spans multiple data platforms.
 *
 * <p>Implementations must be thread-safe. The evaluator is called on the query
 * planning hot path, so implementations should cache policy decisions locally and
 * poll the external engine for updates on a background thread (similar to the
 * Apache Ranger plugin's policy-polling model).
 *
 * <h3>Integration models</h3>
 * <ul>
 *   <li><b>Apache Ranger:</b> implement this interface wrapping
 *       {@code RangerBasePlugin.isAccessAllowed(RangerAccessRequest)}. Ranger's
 *       built-in policy polling ({@code policy.pollIntervalMs}) provides the
 *       background refresh.</li>
 *   <li><b>OPA:</b> implement via HTTP calls to the OPA REST API
 *       ({@code POST /v1/data/<policy-path>}), with a local cache and
 *       configurable TTL.</li>
 * </ul>
 *
 * @see AccessControl
 */
@Experimental(reason = "G-SEC-03: external policy engine integration")
public interface ExternalPolicyEvaluator {

    /**
     * Evaluates whether the given user is allowed to perform the specified action
     * on the resource described by {@code resourceAttributes}.
     *
     * @param user               the authenticated user identity
     * @param action             the action being performed (e.g. "select", "insert", "drop")
     * @param resourceAttributes key-value pairs describing the resource; standard keys are
     *                           {@code "catalog"}, {@code "schema"}, {@code "table"},
     *                           {@code "column"}
     * @return {@code true} if access is allowed, {@code false} if denied
     */
    boolean evaluate(UserIdentity user, String action, Map<String, String> resourceAttributes);

    /**
     * Returns a row filter expression for the given table, if the external policy
     * engine defines one. The expression is injected as a WHERE predicate on scans.
     *
     * @param user    the authenticated user identity
     * @param catalog catalog name
     * @param schema  schema name
     * @param table   table name
     * @return SQL predicate expression, or empty if no row filter applies
     */
    default Optional<String> getRowFilter(UserIdentity user, String catalog,
                                           String schema, String table) {
        return Optional.empty();
    }

    /**
     * Returns a column masking expression for the given column, if the external
     * policy engine defines one.
     *
     * @param user    the authenticated user identity
     * @param catalog catalog name
     * @param schema  schema name
     * @param table   table name
     * @param column  column name
     * @return SQL masking expression, or empty if no mask applies
     */
    default Optional<String> getColumnMask(UserIdentity user, String catalog,
                                            String schema, String table, String column) {
        return Optional.empty();
    }

    /**
     * Returns the set of resource types this evaluator can handle.
     * Typical values: {@code "catalog"}, {@code "schema"}, {@code "table"}, {@code "column"}.
     *
     * @return non-empty set of supported resource types
     */
    Set<String> supportedResourceTypes();

    /**
     * Lifecycle hook: refresh the local policy cache from the external engine.
     * Called periodically by the RBAC refresher alongside the grant-store poll.
     */
    default void refreshPolicies() {
        // no-op by default; implementations with local caches should override
    }

    /**
     * Lifecycle hook: release resources (HTTP clients, thread pools, etc.).
     */
    default void close() {
        // no-op by default
    }
}
