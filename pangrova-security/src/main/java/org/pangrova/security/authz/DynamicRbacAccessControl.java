package org.pangrova.security.authz;

import org.pangrova.security.event.SecurityPolicyChangeEvent;
import org.pangrova.security.event.SecurityPolicyEventPublisher;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dynamic, DB-backed RBAC access control for F-SEC-04.
 *
 * <p>Closes the gap left by the legacy {@link RbacAccessControl}, which was config-only,
 * conflated IdP groups with roles, and could not be updated without restart:
 * <ul>
 *   <li>Grants are resolved from a {@link GrantStore} (typically
 *       {@link org.pangrova.security.store.PersistentGrantStore}) so all coordinators read the
 *       same source of truth.</li>
 *   <li>Identity → role resolution goes through {@link RoleStore} — direct user→role bindings
 *       and group→role mappings — so an IdP group is never silently treated as a role. When
 *       no {@code RoleStore} is supplied this control falls back to the legacy "group ≡ role"
 *       behaviour for backward compatibility with the static path.</li>
 *   <li>{@link SecurityPolicyChangeEvent.GrantChanged} and
 *       {@link SecurityPolicyChangeEvent.RoleCatalogChanged} events trigger immediate cache
 *       invalidation; {@link RbacRefresher} provides a periodic poll for cross-coordinator
 *       eventual consistency within a bounded interval.</li>
 *   <li>Privilege matching uses {@link Privilege#implies(Privilege)} so {@link Privilege#ALL}
 *       and {@link Privilege#OWNER} grants satisfy any specific check.</li>
 * </ul>
 *
 * <p>Delegates row filtering to {@link RowLevelSecurityFilter} and column masking to
 * {@link ColumnMaskingProcessor}.
 */
public class DynamicRbacAccessControl implements AccessControl {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicRbacAccessControl.class);

    private final GrantStore grantStore;
    private final RoleStore roleStore;
    private final AtomicReference<List<PrivilegeGrant>> grantCache;
    private final RowLevelSecurityFilter rlsFilter;
    private final ColumnMaskingProcessor maskProcessor;

    /** Convenience constructor: grant store only, group ≡ role fallback. */
    public DynamicRbacAccessControl(GrantStore grantStore,
                                    RowLevelSecurityFilter rlsFilter,
                                    ColumnMaskingProcessor maskProcessor,
                                    SecurityPolicyEventPublisher eventPublisher) {
        this(grantStore, null, rlsFilter, maskProcessor, eventPublisher);
    }

    /**
     * Full DB-backed constructor with role catalog. Group and role are kept distinct.
     *
     * @param grantStore     authoritative source of privilege grants
     * @param roleStore      role catalog (may be {@code null} → group ≡ role fallback)
     * @param rlsFilter      row-level security filter
     * @param maskProcessor  column masking processor
     * @param eventPublisher policy event bus (may be {@code null} → no event-driven invalidation)
     */
    public DynamicRbacAccessControl(GrantStore grantStore,
                                    RoleStore roleStore,
                                    RowLevelSecurityFilter rlsFilter,
                                    ColumnMaskingProcessor maskProcessor,
                                    SecurityPolicyEventPublisher eventPublisher) {
        if (grantStore == null) {
            throw new IllegalArgumentException("grantStore must not be null");
        }
        this.grantStore = grantStore;
        this.roleStore = roleStore;
        // Best-effort initial load — a transient store failure must not prevent the access
        // control from coming up; the periodic refresher will pick up the snapshot later.
        List<PrivilegeGrant> initial;
        try {
            initial = List.copyOf(grantStore.listGrants());
        } catch (RuntimeException e) {
            LOG.warn("event=rbac_initial_load_failed using_empty_cache error={}", e.getMessage(), e);
            initial = List.of();
        }
        this.grantCache = new AtomicReference<>(initial);
        this.rlsFilter = rlsFilter;
        this.maskProcessor = maskProcessor;
        if (eventPublisher != null) {
            eventPublisher.subscribe(this::onPolicyEvent);
        }
    }

    /**
     * Re-loads the grant cache from the underlying store. Called by the event subscriber on
     * {@code GrantChanged} / {@code RoleCatalogChanged} and by {@link RbacRefresher}.
     * Safe to invoke from multiple threads — uses a single atomic reference swap.
     */
    public void refreshGrants() {
        try {
            List<PrivilegeGrant> snapshot = List.copyOf(grantStore.listGrants());
            grantCache.set(snapshot);
            LOG.debug("event=rbac_grants_refreshed count={}", snapshot.size());
        } catch (RuntimeException e) {
            LOG.warn("event=rbac_grants_refresh_failed error={}", e.getMessage(), e);
        }
    }

    private void onPolicyEvent(SecurityPolicyChangeEvent event) {
        // Refresh on any policy event. Cache invalidation is cheap (single AtomicReference
        // swap after one SELECT) and over-refreshing is strictly safer than missing a change.
        refreshGrants();
    }

    /**
     * Resolves the effective set of role names for the given identity.
     *
     * <ul>
     *     <li>If a {@link RoleStore} is configured: union of {@code roleStore.rolesForUser(principal)}
     *         and {@code roleStore.rolesForGroup(g)} for each group in {@code user.groups()} —
     *         group and role are kept distinct.</li>
     *     <li>Otherwise: legacy fallback — {@code user.groups()} is treated as the role set.</li>
     * </ul>
     */
    public Set<String> effectiveRoles(UserIdentity user) {
        if (roleStore == null) {
            return user.groups();
        }
        Set<String> roles = new HashSet<>(roleStore.rolesForUser(user.principal()));
        for (String group : user.groups()) {
            roles.addAll(roleStore.rolesForGroup(group));
        }
        return Set.copyOf(roles);
    }

    @Override
    public void checkCanAccessCatalog(UserIdentity user, String catalog) {
        requireAnyGrant(user, catalog, null, null, Privilege.SELECT);
    }

    @Override
    public void checkCanAccessSchema(UserIdentity user, String catalog, String schema) {
        requireAnyGrant(user, catalog, schema, null, Privilege.SELECT);
    }

    @Override
    public void checkCanSelectFromTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.SELECT);
    }

    @Override
    public void checkCanInsertIntoTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.INSERT);
    }

    @Override
    public void checkCanDeleteFromTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.DELETE);
    }

    @Override
    public void checkCanDropTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.DROP);
    }

    @Override
    public void checkCanAlterTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.ALTER);
    }

    @Override
    public void checkCanCreateTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.CREATE_TABLE);
    }

    @Override
    public void checkCanRenameTable(UserIdentity user,
                                    String catalog, String schema, String oldTable,
                                    String newCatalog, String newSchema, String newTable) {
        requireTableGrant(user, catalog, schema, oldTable, Privilege.RENAME_TABLE);
        requireTableGrant(user, newCatalog, newSchema, newTable, Privilege.CREATE_TABLE);
    }

    @Override
    public void checkCanCreateSchema(UserIdentity user, String catalog, String schema) {
        requireSchemaGrant(user, catalog, schema, Privilege.CREATE_SCHEMA);
    }

    @Override
    public void checkCanDropSchema(UserIdentity user, String catalog, String schema) {
        requireSchemaGrant(user, catalog, schema, Privilege.DROP_SCHEMA);
    }

    @Override
    public void checkCanAlterSchema(UserIdentity user, String catalog, String schema) {
        requireSchemaGrant(user, catalog, schema, Privilege.ALTER_SCHEMA);
    }

    @Override
    public void checkCanShowSchemas(UserIdentity user, String catalog) {
        requireAnyGrant(user, catalog, null, null, Privilege.SHOW_SCHEMAS);
    }

    @Override
    public void checkCanShowTables(UserIdentity user, String catalog, String schema) {
        requireSchemaGrant(user, catalog, schema, Privilege.SHOW_TABLES);
    }

    @Override
    public void checkCanShowColumns(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.SHOW_COLUMNS);
    }

    @Override
    public void checkCanSelectFromColumns(UserIdentity user, String catalog, String schema,
                                          String table, Set<String> columns) {
        List<PrivilegeGrant> userGrants = resolveGrants(user);
        boolean hasTableSelect = userGrants.stream()
                .anyMatch(g -> g.privilege().implies(Privilege.SELECT) && g.coversTable(catalog, schema, table));
        if (hasTableSelect) {
            return;
        }
        for (String col : columns) {
            checkColumnSelect(userGrants, catalog, schema, table, col);
        }
    }

    @Override
    public Optional<String> getRowFilter(UserIdentity user, String catalog, String schema, String table) {
        return rlsFilter.getFilter(user, catalog, schema, table);
    }

    @Override
    public Optional<String> getColumnMask(UserIdentity user, String catalog, String schema,
                                          String table, String column) {
        return maskProcessor.getMask(user, catalog, schema, table, column);
    }

    private List<PrivilegeGrant> resolveGrants(UserIdentity user) {
        Set<String> roles = effectiveRoles(user);
        return grantCache.get().stream()
                .filter(g -> roles.contains(g.roleName()))
                .toList();
    }

    private void requireTableGrant(UserIdentity user, String catalog, String schema,
                                   String table, Privilege privilege) {
        List<PrivilegeGrant> userGrants = resolveGrants(user);
        PrivilegeGrant matchedGrant = userGrants.stream()
                .filter(g -> g.privilege().implies(privilege) && g.coversTable(catalog, schema, table))
                .findFirst()
                .orElse(null);
        if (matchedGrant == null) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks %s on %s.%s.%s".formatted(user.principal(), privilege, catalog, schema, table));
        }
        logWildcardGrantAccess(user, matchedGrant, catalog, schema, table);
    }

    /**
     * Logs a warning when access is granted via a wildcard pattern, to aid
     * audit trail analysis for over-privileged grants.
     */
    private static void logWildcardGrantAccess(UserIdentity user, PrivilegeGrant grant,
                                               String catalog, String schema, String table) {
        boolean hasWildcard = "*".equals(grant.catalog())
                || "*".equals(grant.schema())
                || "*".equals(grant.table());
        if (hasWildcard) {
            LOG.info("event=wildcard_grant_access principal={} privilege={} "
                    + "grantPattern={}.{}.{} accessedResource={}.{}.{}",
                    user.principal(), grant.privilege(),
                    grant.catalog(), grant.schema(), grant.table(),
                    catalog, schema, table);
        }
    }

    private void requireSchemaGrant(UserIdentity user, String catalog, String schema,
                                    Privilege privilege) {
        boolean granted = resolveGrants(user).stream()
                .anyMatch(g -> g.privilege().implies(privilege) && g.coversTable(catalog, schema, null));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks %s on %s.%s".formatted(user.principal(), privilege, catalog, schema));
        }
    }

    private void requireAnyGrant(UserIdentity user, String catalog, String schema,
                                 String table, Privilege privilege) {
        boolean granted = resolveGrants(user).stream()
                .anyMatch(g -> g.privilege().implies(privilege) && g.coversTable(catalog, schema, table));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks %s on catalog '%s'".formatted(user.principal(), privilege, catalog));
        }
    }

    private void checkColumnSelect(List<PrivilegeGrant> userGrants, String catalog,
                                   String schema, String table, String column) {
        boolean granted = userGrants.stream()
                .anyMatch(g -> g.privilege().implies(Privilege.SELECT)
                    && g.coversColumn(catalog, schema, table, column));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User lacks SELECT on column '%s' in %s.%s.%s".formatted(column, catalog, schema, table));
        }
    }
}
