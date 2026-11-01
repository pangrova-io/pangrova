package org.pangrova.security.authz;

import org.pangrova.spi.security.policy.SqlIdentifierValidator;
import org.pangrova.spi.security.event.SecurityPolicyChangeEvent;
import org.pangrova.spi.security.event.SecurityPolicyEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.pangrova.persistence.api.security.policy.GrantStore;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.security.policy.PrivilegeGrant;

/**
 * Handles GRANT and REVOKE DDL statements by mutating the backing {@link GrantStore}
 * and publishing {@link SecurityPolicyChangeEvent.GrantChanged} events so all
 * coordinators invalidate their RBAC caches.
 *
 * <p>This is the execution-side counterpart of the parser-side
 * {@code GrantPrivilegeStatement} / {@code RevokePrivilegeStatement} AST nodes.
 * The coordinator's statement dispatch routes those nodes here.
 *
 * <p>Input validation: privilege strings are resolved via {@link Privilege#valueOf},
 * so unknown privileges produce an {@link IllegalArgumentException}. Object identifiers
 * are validated via {@link SqlIdentifierValidator}.
 */
public final class SecurityStatementHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityStatementHandler.class);

    private final GrantStore grantStore;
    private final SecurityPolicyEventPublisher eventPublisher;

    public SecurityStatementHandler(GrantStore grantStore,
                                     SecurityPolicyEventPublisher eventPublisher) {
        this.grantStore = Objects.requireNonNull(grantStore, "grantStore must not be null");
        this.eventPublisher = Objects.requireNonNull(eventPublisher, "eventPublisher must not be null");
    }

    /**
     * The object a GRANT/REVOKE applies to.
     *
     * <p>These four fields always travel together — {@link PrivilegeGrant} and
     * {@link GrantStore#revokeGrants} take them in this exact order — so they are passed as one
     * value instead of four positional parameters, which also keeps the statement methods within
     * the project parameter-count budget.</p>
     *
     * @param objectType TABLE, SCHEMA, or CATALOG
     * @param catalog    catalog name ({@code null} or {@code *} for wildcards)
     * @param schema     schema name ({@code null} for catalog-level grants)
     * @param table      table name ({@code null} for schema-level grants)
     */
    public record GrantTarget(String objectType, String catalog, String schema, String table) {
    }

    /**
     * Executes a GRANT privilege statement.
     *
     * @param privilege       privilege name (must match {@link Privilege} enum)
     * @param target          the object the privilege is granted on
     * @param roleName        grantee role
     * @param withGrantOption if true, grantee can re-grant
     * @param grantedBy       principal executing the GRANT
     */
    public void executeGrant(String privilege, GrantTarget target, String roleName,
                             boolean withGrantOption, String grantedBy) {
        Privilege priv = resolvePrivilege(privilege);
        validateIdentifiers(target.catalog(), target.schema(), target.table(), roleName);

        PrivilegeGrant grant = new PrivilegeGrant(
                null, roleName, priv, target.objectType(),
                target.catalog(), target.schema(), target.table(), null,
                withGrantOption, false, grantedBy, Instant.now());

        grantStore.addGrant(grant);

        eventPublisher.publish(new SecurityPolicyChangeEvent.GrantChanged(
                grantedBy, roleName, priv.name()));

        LOG.info("event=grant_executed privilege={} object={}.{}.{} role={} by={}",
                priv, target.catalog(), target.schema(), target.table(), roleName, grantedBy);
    }

    /**
     * Executes a REVOKE privilege statement.
     *
     * @param privilege privilege name (must match {@link Privilege} enum)
     * @param target    the object the privilege is revoked on
     * @param roleName  role to revoke from
     * @param revokedBy principal executing the REVOKE
     * @return the number of grants revoked
     */
    public int executeRevoke(String privilege, GrantTarget target, String roleName,
                             String revokedBy) {
        Privilege priv = resolvePrivilege(privilege);
        validateIdentifiers(target.catalog(), target.schema(), target.table(), roleName);

        int revoked = grantStore.revokeGrants(
                roleName, priv, target.catalog(), target.schema(), target.table());

        if (revoked > 0) {
            eventPublisher.publish(new SecurityPolicyChangeEvent.GrantChanged(
                    revokedBy, roleName, priv.name()));
        }

        LOG.info("event=revoke_executed privilege={} object={}.{}.{} role={} revoked={} by={}",
                priv, target.catalog(), target.schema(), target.table(), roleName, revoked, revokedBy);
        return revoked;
    }

    /**
     * Returns the privilege names this handler supports (for introspection / help).
     */
    public List<String> supportedPrivileges() {
        return java.util.Arrays.stream(Privilege.values())
                .map(Privilege::name)
                .toList();
    }

    private Privilege resolvePrivilege(String privilege) {
        try {
            return Privilege.valueOf(privilege.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown privilege '%s'. Supported: %s".formatted(
                            privilege, supportedPrivileges()));
        }
    }

    private void validateIdentifiers(String catalog, String schema, String table, String roleName) {
        if (catalog != null && !"*".equals(catalog)) {
            SqlIdentifierValidator.validateIdentifier(catalog);
        }
        if (schema != null && !"*".equals(schema)) {
            SqlIdentifierValidator.validateIdentifier(schema);
        }
        if (table != null && !"*".equals(table)) {
            SqlIdentifierValidator.validateIdentifier(table);
        }
        SqlIdentifierValidator.validateIdentifier(roleName);
    }
}
