package org.pangrova.coordinator.query.execution.dispatch;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.statement.StatementTestSessions;
import org.pangrova.parser.SqlParser;
import org.pangrova.security.authz.AuthorizationDeniedException;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.InMemoryRoleStore;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.StaticGrantStore;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.mask.EmptyColumnMaskStore;
import org.pangrova.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Uses the real RBAC implementation and direct principal-role assignments, not permission mocks. */
class PermissionCheckerAuthorizationTest {
    private static final CoordinatorSessionContext SESSION = session("secured", "source_schema");

    @Test
    void createAndCtasRequireCreatePrivilegeOnTheActualQueryDefaultTarget() {
        var create = new SqlParser().parse("CREATE TABLE docs (id BIGINT)");
        var ctas = new SqlParser().parse("CREATE TABLE docs AS SELECT 1 AS id");
        var alterOnly = accessControl(grant(Privilege.ALTER, "secured", "source_schema", "docs"));
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, alterOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_TABLE");
        assertThatThrownBy(() -> PermissionChecker.check(ctas, SESSION, alterOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_TABLE");
        var allowed = accessControl(grant(Privilege.CREATE_TABLE, "secured", "source_schema", "docs"));
        assertThatCode(() -> PermissionChecker.check(create, SESSION, allowed)).doesNotThrowAnyException();
        assertThatCode(() -> PermissionChecker.check(ctas, SESSION, allowed)).doesNotThrowAnyException();
        var wrongDefault = accessControl(grant(Privilege.CREATE_TABLE, "default", "default", "docs"));
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, wrongDefault))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("secured.source_schema.docs");
    }

    @Test
    void createSchemaRequiresItsExactCatalogAndSchemaPrivilege() {
        var create = new SqlParser().parse("CREATE SCHEMA new_schema");
        var selectOnly = accessControl(grant(Privilege.SELECT, "secured", "new_schema", null));
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, selectOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_SCHEMA");
        var allowed = accessControl(grant(Privilege.CREATE_SCHEMA, "secured", "new_schema", null));
        assertThatCode(() -> PermissionChecker.check(create, SESSION, allowed)).doesNotThrowAnyException();
        var other = new SqlParser().parse("CREATE SCHEMA other_catalog.new_schema");
        assertThatThrownBy(() -> PermissionChecker.check(other, SESSION, allowed))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("other_catalog");
    }

    @Test
    void catalogMutationsRequireTheirOwnPrivilegeOnTheExactCatalog() {
        var create = new SqlParser().parse("CREATE CATALOG fresh WITH ('type' = 'lance')");
        var drop = new SqlParser().parse("DROP CATALOG fresh");
        var selectOnly = accessControl(grant(Privilege.SELECT, "fresh", null, null));
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, selectOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_CATALOG");
        assertThatThrownBy(() -> PermissionChecker.check(drop, SESSION, selectOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("DROP_CATALOG");
        var createOnly = accessControl(grant(Privilege.CREATE_CATALOG, "fresh", null, null));
        assertThatCode(() -> PermissionChecker.check(create, SESSION, createOnly)).doesNotThrowAnyException();
        assertThatThrownBy(() -> PermissionChecker.check(drop, SESSION, createOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("DROP_CATALOG");
        var dropOnly = accessControl(grant(Privilege.DROP_CATALOG, "fresh", null, null));
        assertThatCode(() -> PermissionChecker.check(drop, SESSION, dropOnly)).doesNotThrowAnyException();
        var wrongCatalog = accessControl(grant(Privilege.CREATE_CATALOG, "other", null, null));
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, wrongCatalog))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("fresh");
    }

    @Test
    void renameRequiresSourceRenameAndExactDestinationCreatePrivileges() {
        var rename = new SqlParser().parse("ALTER TABLE secured.source_schema.docs RENAME TO target_schema.renamed");
        var sourceOnly = accessControl(grant(Privilege.RENAME_TABLE, "secured", "source_schema", "docs"));
        assertThatThrownBy(() -> PermissionChecker.check(rename, SESSION, sourceOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_TABLE")
                .hasMessageContaining("secured.target_schema.renamed");
        var allowed = accessControl(grant(Privilege.RENAME_TABLE, "secured", "source_schema", "docs"),
                grant(Privilege.CREATE_TABLE, "secured", "target_schema", "renamed"));
        assertThatCode(() -> PermissionChecker.check(rename, SESSION, allowed)).doesNotThrowAnyException();
        var crossCatalog = new SqlParser().parse("ALTER TABLE secured.source_schema.docs RENAME TO other.target.renamed");
        assertThatThrownBy(() -> PermissionChecker.check(crossCatalog, SESSION, allowed))
                .hasMessageContaining("cannot move a table between catalogs");
    }

    private static CoordinatorSessionContext session(String catalog, String schema) {
        var authenticated = StatementTestSessions.context();
        return new CoordinatorSessionContext(authenticated.queryId(), Map.of("catalog", catalog, "schema", schema),
                authenticated.userIdentity(), authenticated.tenantId());
    }

    private static PrivilegeGrant grant(Privilege privilege, String catalog, String schema, String table) {
        String objectType = schema == null ? "CATALOG" : table == null ? "SCHEMA" : "TABLE";
        return PrivilegeGrant.of("ddl-role", privilege, objectType, catalog, schema, table, null);
    }

    private static DynamicRbacAccessControl accessControl(PrivilegeGrant... grants) {
        var roles = new InMemoryRoleStore();
        roles.createRole(Role.of("ddl-role"));
        roles.grantRoleToUser(UserRoleAssignment.of(SESSION.userIdentity().principal(), "ddl-role"));
        return new DynamicRbacAccessControl(new StaticGrantStore(Arrays.asList(grants)), roles,
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }
}
