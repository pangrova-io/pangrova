package org.pangrova.security.core.authz;

import org.pangrova.spi.security.AuthorizationDeniedException;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.common.file.FileSpaceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Objects;
import org.pangrova.spi.security.mask.ColumnMaskSource;
import org.pangrova.persistence.api.security.policy.GrantStore;
import org.pangrova.spi.security.policy.Privilege;
import org.pangrova.spi.security.policy.PrivilegeGrant;
import org.pangrova.persistence.api.security.policy.RoleStore;
import org.pangrova.spi.security.rowfilter.RowFilterSource;

/**
 * Current authorization from the authoritative role catalog and privilege store.
 * Every check reads current grants; storage failures never reuse an earlier permission.
 * Identity groups remain distinct from explicitly resolved Pangrova roles.
 */
public final class DynamicRbacAccessControl implements AccessControl {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicRbacAccessControl.class);

    private final GrantStore grantStore;
    private final RoleStore roleStore;
    private final RowFilterSource rlsFilter;
    private final ColumnMaskSource maskProcessor;

    /** Creates the sole RBAC path with a mandatory role authority. */
    public DynamicRbacAccessControl(GrantStore grantStore, RoleStore roleStore,
                                    RowFilterSource rlsFilter,
                                    ColumnMaskSource maskProcessor) {
        this.grantStore = Objects.requireNonNull(grantStore, "grantStore");
        this.roleStore = Objects.requireNonNull(roleStore, "roleStore");
        this.rlsFilter = Objects.requireNonNull(rlsFilter, "rlsFilter");
        this.maskProcessor = Objects.requireNonNull(maskProcessor, "maskProcessor");
    }

    /** Resolves current direct assignments and explicitly mapped identity groups. */
    public Set<String> effectiveRoles(UserIdentity user) {
        if (user == null || user.principal() == null || user.principal().isBlank()) {
            throw new AuthorizationDeniedException("Authenticated principal is required");
        }
        Set<String> roles = new HashSet<>(roleStore.rolesForUser(user.principal()));
        for (String group : user.groups()) {
            roles.addAll(roleStore.rolesForGroup(group));
        }
        roles.removeIf(role -> roleStore.findRole(role).isEmpty());
        return Set.copyOf(roles);
    }

    @Override
    public void checkCanAccessCatalog(UserIdentity user, String catalog) {
        requireAnyGrant(user, catalog, null, null, Privilege.SELECT);
    }

    @Override
    public void checkCanCreateCatalog(UserIdentity user, String catalog) {
        requireCatalogGrant(user, catalog, Privilege.CREATE_CATALOG);
    }

    @Override
    public void checkCanDropCatalog(UserIdentity user, String catalog) {
        requireCatalogGrant(user, catalog, Privilege.DROP_CATALOG);
    }

    @Override
    public void checkCanAlterCatalog(UserIdentity user, String catalog) {
        requireCatalogGrant(user, catalog, Privilege.ALTER_CATALOG);
    }

    @Override
    public void checkCanManageStorageProfile(UserIdentity user, String profileName) {
        boolean granted = resolveGrants(user).stream().anyMatch(grant ->
                grant.privilege().implies(Privilege.MANAGE_STORAGE_PROFILE)
                        && grant.coversStorageProfile(profileName));
        if (!granted) {
            throw new AuthorizationDeniedException("User '%s' lacks MANAGE_STORAGE_PROFILE on storage profile '%s'"
                    .formatted(user.principal(), profileName));
        }
    }

    @Override
    public void checkCanViewStorageProfile(UserIdentity user, String profileName) {
        boolean granted = resolveGrants(user).stream().anyMatch(grant ->
                grant.coversStorageProfile(profileName)
                        && (grant.privilege().implies(Privilege.VIEW_STORAGE_PROFILE)
                        || grant.privilege() == Privilege.MANAGE_STORAGE_PROFILE));
        if (!granted) {
            throw new AuthorizationDeniedException("User '%s' lacks VIEW_STORAGE_PROFILE on storage profile '%s'"
                    .formatted(user.principal(), profileName));
        }
    }

    @Override
    public void checkCanReadStorageProfile(UserIdentity user, String profileName) {
        boolean granted = resolveGrants(user).stream().anyMatch(grant ->
                grant.privilege().implies(Privilege.READ_STORAGE_PROFILE)
                        && grant.coversStorageProfile(profileName));
        if (!granted) {
            throw new AuthorizationDeniedException("User '%s' lacks READ_STORAGE_PROFILE on storage profile '%s'"
                    .formatted(user.principal(), profileName));
        }
    }

    @Override
    public void checkCanAccessSchema(UserIdentity user, String catalog, String schema) {
        requireAnyGrant(user, catalog, schema, null, Privilege.SELECT);
    }

    @Override
    public void checkCanCreateVolume(UserIdentity user, String catalog, String schema,
            String name, boolean external) {
        requireFileSpaceGrant(user, FileSpaceType.VOLUME, catalog, schema, null,
                external ? Privilege.CREATE_EXTERNAL_VOLUME : Privilege.CREATE_VOLUME);
    }

    @Override
    public void checkCanReadVolume(UserIdentity user, String catalog, String schema,
            String name) {
        requireFileSpaceGrant(user, FileSpaceType.VOLUME, catalog, schema, name,
                Privilege.READ_VOLUME);
    }

    @Override
    public void checkCanWriteVolume(UserIdentity user, String catalog, String schema,
            String name) {
        requireFileSpaceGrant(user, FileSpaceType.VOLUME, catalog, schema, name,
                Privilege.WRITE_VOLUME);
    }

    @Override
    public void checkCanManageVolume(UserIdentity user, String catalog, String schema,
            String name) {
        requireFileSpaceGrant(user, FileSpaceType.VOLUME, catalog, schema, name,
                Privilege.MANAGE_VOLUME);
    }

    @Override
    public void checkCanCreateStage(UserIdentity user, String catalog, String schema,
            String name) {
        requireFileSpaceGrant(user, FileSpaceType.STAGE, catalog, schema, null,
                Privilege.CREATE_STAGE);
    }

    @Override
    public void checkCanUseStage(UserIdentity user, String catalog, String schema,
            String name) {
        requireFileSpaceGrant(user, FileSpaceType.STAGE, catalog, schema, name,
                Privilege.USAGE_STAGE);
    }

    @Override
    public void checkCanReadStage(UserIdentity user, String catalog, String schema,
            String name) {
        requireFileSpaceGrant(user, FileSpaceType.STAGE, catalog, schema, name,
                Privilege.READ_STAGE);
    }

    @Override
    public void checkCanWriteStage(UserIdentity user, String catalog, String schema,
            String name) {
        requireFileSpaceGrant(user, FileSpaceType.STAGE, catalog, schema, name,
                Privilege.WRITE_STAGE);
    }

    @Override
    public void checkCanManageStage(UserIdentity user, String catalog, String schema,
            String name) {
        boolean granted = resolveGrants(user).stream().anyMatch(grant ->
                grant.privilege() == Privilege.OWNER
                        && grant.coversFileSpace(FileSpaceType.STAGE, catalog, schema, name));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks OWNER on stage %s.%s.%s"
                            .formatted(user.principal(), catalog, schema, name));
        }
    }

    @Override
    public void checkCanSelectFromTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.SELECT);
    }

    @Override
    public void checkCanReadTableMetadata(UserIdentity user, String catalog, String schema, String table) {
        Set<String> roles = effectiveRoles(user);
        requireTableGrant(user, resolveGrants(roles), catalog, schema, table, Privilege.SELECT);
        if (rlsFilter.getFilter(user, roles, catalog, schema, table).isPresent()
                || maskProcessor.hasApplicableMask(user, roles, catalog, schema, table)) {
            throw new AuthorizationDeniedException(
                    "Unfiltered table metadata cannot bypass current row or column policies on "
                            + catalog + "." + schema + "." + table);
        }
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
    public void checkCanUpdateTable(UserIdentity user, String catalog, String schema, String table) {
        requireTableGrant(user, catalog, schema, table, Privilege.UPDATE);
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
        return rlsFilter.getFilter(user, effectiveRoles(user), catalog, schema, table);
    }

    @Override
    public Optional<String> getColumnMask(UserIdentity user, String catalog, String schema,
                                          String table, String column) {
        return maskProcessor.getMask(user, effectiveRoles(user), catalog, schema, table, column);
    }

    private List<PrivilegeGrant> resolveGrants(UserIdentity user) {
        return resolveGrants(effectiveRoles(user));
    }

    private List<PrivilegeGrant> resolveGrants(Set<String> roles) {
        return grantStore.listGrants().stream()
                .filter(g -> roles.contains(g.roleName()))
                .toList();
    }

    private void requireTableGrant(UserIdentity user, String catalog, String schema,
                                   String table, Privilege privilege) {
        requireTableGrant(user, resolveGrants(user), catalog, schema, table, privilege);
    }

    private static void requireTableGrant(UserIdentity user, List<PrivilegeGrant> userGrants,
            String catalog, String schema, String table, Privilege privilege) {
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

    private void requireFileSpaceGrant(UserIdentity user, FileSpaceType type, String catalog,
            String schema, String name, Privilege privilege) {
        boolean granted = resolveGrants(user).stream().anyMatch(grant ->
                grant.privilege().implies(privilege)
                        && grant.coversFileSpace(type, catalog, schema, name));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks %s on %s %s.%s.%s".formatted(user.principal(), privilege,
                            type, catalog, schema, name == null ? "*" : name));
        }
    }

    private void requireSchemaGrant(UserIdentity user, String catalog, String schema,
            Privilege privilege) {
        boolean granted = resolveGrants(user).stream()
                .anyMatch(g -> g.privilege().implies(privilege)
                        && g.coversTable(catalog, schema, null));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks %s on %s.%s".formatted(user.principal(), privilege, catalog, schema));
        }
    }

    private void requireCatalogGrant(UserIdentity user, String catalog, Privilege privilege) {
        boolean granted = resolveGrants(user).stream().anyMatch(grant ->
                "CATALOG".equalsIgnoreCase(grant.objectType())
                        && grant.privilege().implies(privilege) && grant.coversTable(catalog, null, null));
        if (!granted) {
            throw new AuthorizationDeniedException(
                    "User '%s' lacks %s on catalog '%s'".formatted(user.principal(), privilege, catalog));
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
