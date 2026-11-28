package org.pangrova.coordinator.query.execution.dispatch;

import org.junit.jupiter.api.Test;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.statement.StatementTestSessions;
import org.pangrova.parser.syntax.AntlrSqlParser;
import org.pangrova.security.api.exception.AuthorizationDeniedException;
import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;
import org.pangrova.testing.testkit.security.policy.InMemoryRoleStore;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.testing.testkit.security.policy.StaticGrantStore;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.testing.testkit.security.mask.EmptyColumnMaskStore;
import org.pangrova.testing.testkit.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.file.api.enums.FileSpaceType;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Uses the real RBAC implementation and direct principal-role assignments, not permission mocks. */
class PermissionCheckerAuthorizationTest {
    private static final QuerySession SESSION = session("secured", "source_schema");

    @Test
    void viewsRequireExactCreateReplaceAndDropAuthority() {
        var parser = new AntlrSqlParser();
        var create = parser.parse("CREATE VIEW docs AS SELECT 1 AS id");
        var replace = parser.parse("CREATE OR REPLACE VIEW docs AS SELECT 2 AS id");
        var drop = parser.parse("DROP VIEW IF EXISTS docs");
        var tableCreator = accessControl(grant(Privilege.CREATE_TABLE, "secured", "source_schema", "docs"));
        var viewCreator = accessControl(grant(Privilege.CREATE_VIEW, "secured", "source_schema", "docs"));
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, tableCreator))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_VIEW");
        assertThatCode(() -> PermissionChecker.check(create, SESSION, viewCreator)).doesNotThrowAnyException();
        assertThatThrownBy(() -> PermissionChecker.check(replace, SESSION, viewCreator))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("ALTER");
        assertThatThrownBy(() -> PermissionChecker.check(drop, SESSION, viewCreator))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("DROP");
        var editor = accessControl(grant(Privilege.CREATE_VIEW, "secured", "source_schema", "docs"),
                grant(Privilege.ALTER, "secured", "source_schema", "docs"));
        assertThatCode(() -> PermissionChecker.check(replace, SESSION, editor)).doesNotThrowAnyException();
        assertThatCode(() -> PermissionChecker.check(drop, SESSION,
                accessControl(grant(Privilege.DROP, "secured", "source_schema", "docs"))))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, null))
                .hasMessageContaining("Authenticated view");
        assertThatThrownBy(() -> PermissionChecker.check(create, null, viewCreator))
                .hasMessageContaining("Authenticated view");
        assertThatThrownBy(() -> PermissionChecker.check(parser.parse("CREATE VIEW other.docs AS SELECT 1"),
                SESSION, viewCreator)).isInstanceOf(AuthorizationDeniedException.class);
    }

    @Test
    void maintenanceAndCallRequirePrivilegesOnTheirActualTarget() {
        var selectOnly = accessControl(grant(Privilege.SELECT, "secured", "source_schema", "docs"));
        var alter = accessControl(grant(Privilege.ALTER, "secured", "source_schema", "docs"));
        for (String sql : new String[]{"COMPACT docs", "VACUUM docs", "OPTIMIZE docs",
                "CALL secured.compact('source_schema.docs')"}) {
            var statement = new AntlrSqlParser().parse(sql);
            assertThatThrownBy(() -> PermissionChecker.check(statement, SESSION, selectOnly))
                    .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("ALTER");
            assertThatCode(() -> PermissionChecker.check(statement, SESSION, alter)).doesNotThrowAnyException();
        }
        var warmup = new AntlrSqlParser().parse("CALL secured.warmup_table('source_schema', 'docs')");
        assertThatCode(() -> PermissionChecker.check(warmup, SESSION, selectOnly)).doesNotThrowAnyException();
        assertThatThrownBy(() -> PermissionChecker.check(warmup, SESSION, alter))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("SELECT");
        var other = new AntlrSqlParser().parse("CALL another.compact('source_schema.docs')");
        assertThatThrownBy(() -> PermissionChecker.check(other, SESSION, alter))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("another");
    }

    @Test
    void createAndCtasRequireCreatePrivilegeOnTheActualQueryDefaultTarget() {
        var create = new AntlrSqlParser().parse("CREATE TABLE docs (id BIGINT)");
        var ctas = new AntlrSqlParser().parse("CREATE TABLE docs AS SELECT 1 AS id");
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
    void dynamicTableLifecycleUsesCreateAlterAndDropPrivilegesOnQualifiedName() {
        var create = new AntlrSqlParser().parse("CREATE DYNAMIC TABLE secured.source_schema.rollup "
                + "AS SELECT 1 AS id");
        var refresh = new AntlrSqlParser().parse("ALTER DYNAMIC TABLE secured.source_schema.rollup REFRESH");
        var drop = new AntlrSqlParser().parse("DROP DYNAMIC TABLE secured.source_schema.rollup");
        var createOnly = accessControl(grant(Privilege.CREATE_TABLE,
                "secured", "source_schema", "rollup"));
        assertThatCode(() -> PermissionChecker.check(create, SESSION, createOnly)).doesNotThrowAnyException();
        assertThatThrownBy(() -> PermissionChecker.check(refresh, SESSION, createOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("ALTER");
        assertThatThrownBy(() -> PermissionChecker.check(drop, SESSION, createOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("DROP");
    }

    @Test
    void physicalGenerationNamesAreReservedEvenForWildcardTableGrants() {
        var broad = accessControl(grant(Privilege.ALL, "*", "*", "*"));
        for (String sql : new String[]{
                "CREATE TABLE secured.source_schema.__pg_dt_spoof (id BIGINT)",
                "DROP TABLE secured.source_schema.__pg_dt_spoof",
                "INSERT INTO secured.source_schema.__pg_dt_spoof VALUES (1)",
                "ALTER TABLE secured.source_schema.docs RENAME TO __pg_dt_spoof"}) {
            assertThatThrownBy(() -> PermissionChecker.check(new AntlrSqlParser().parse(sql), SESSION, broad))
                    .isInstanceOf(org.pangrova.security.api.exception.AccessDeniedException.class)
                    .hasMessageContaining("reserved");
        }
    }

    @Test
    void createSchemaRequiresItsExactCatalogAndSchemaPrivilege() {
        var create = new AntlrSqlParser().parse("CREATE SCHEMA new_schema");
        var selectOnly = accessControl(grant(Privilege.SELECT, "secured", "new_schema", null));
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, selectOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_SCHEMA");
        var allowed = accessControl(grant(Privilege.CREATE_SCHEMA, "secured", "new_schema", null));
        assertThatCode(() -> PermissionChecker.check(create, SESSION, allowed)).doesNotThrowAnyException();
        var other = new AntlrSqlParser().parse("CREATE SCHEMA other_catalog.new_schema");
        assertThatThrownBy(() -> PermissionChecker.check(other, SESSION, allowed))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("other_catalog");
    }

    @Test
    void catalogMutationsRequireTheirOwnPrivilegeOnTheExactCatalog() {
        var create = new AntlrSqlParser().parse("CREATE CATALOG fresh WITH ('type' = 'lance')");
        var drop = new AntlrSqlParser().parse("DROP CATALOG fresh");
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
    void storageProfileMutationsRequireTheirOwnScopedManagementGrant() {
        var create = new AntlrSqlParser().parse("CREATE STORAGE PROFILE staging "
                + "WITH ('type'='S3', 'bucket'='example')");
        var drop = new AntlrSqlParser().parse("DROP STORAGE PROFILE IF EXISTS staging");
        var tableAdmin = accessControl(grant(Privilege.ALL, "*", "*", "*"));
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, tableAdmin))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("MANAGE_STORAGE_PROFILE");
        assertThatThrownBy(() -> PermissionChecker.check(drop, SESSION, tableAdmin))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("MANAGE_STORAGE_PROFILE");

        var granted = accessControl(PrivilegeGrant.of("ddl-role", Privilege.MANAGE_STORAGE_PROFILE,
                "STORAGE_PROFILE", "staging", null, null, null));
        assertThatCode(() -> PermissionChecker.check(create, SESSION, granted)).doesNotThrowAnyException();
        assertThatCode(() -> PermissionChecker.check(drop, SESSION, granted)).doesNotThrowAnyException();
        var other = new AntlrSqlParser().parse("DROP STORAGE PROFILE IF EXISTS unrelated");
        assertThatThrownBy(() -> PermissionChecker.check(other, SESSION, granted))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("unrelated");
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, null))
                .hasMessageContaining("Authenticated storage profile administrator");
        assertThatThrownBy(() -> PermissionChecker.check(create, null, granted))
                .hasMessageContaining("Authenticated storage profile administrator");
    }

    @Test
    void volumeDdlUsesSchemaScopedCreationAndExactManagementPrivileges() {
        var managed = new AntlrSqlParser().parse("CREATE VOLUME media.raw.landing");
        var external = new AntlrSqlParser().parse("CREATE EXTERNAL VOLUME media.raw.landing "
                + "LOCATION 's3://acme/landing/' STORAGE_PROFILE 'prod-s3'");
        var alter = new AntlrSqlParser().parse("ALTER VOLUME media.raw.landing SET COMMENT 'x'");
        var drop = new AntlrSqlParser().parse("DROP VOLUME IF EXISTS media.raw.landing");

        var managedCreate = accessControl(spaceGrant(Privilege.CREATE_VOLUME,
                FileSpaceType.VOLUME, "media", "raw", null));
        assertThatCode(() -> PermissionChecker.check(managed, SESSION, managedCreate))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> PermissionChecker.check(external, SESSION, managedCreate))
                .isInstanceOf(AuthorizationDeniedException.class)
                .hasMessageContaining("CREATE_EXTERNAL_VOLUME");

        var externalCreate = accessControl(spaceGrant(Privilege.CREATE_EXTERNAL_VOLUME,
                FileSpaceType.VOLUME, "media", "raw", null));
        assertThatCode(() -> PermissionChecker.check(external, SESSION, externalCreate))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> PermissionChecker.check(managed, SESSION, externalCreate))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_VOLUME");

        var wrongSchema = accessControl(spaceGrant(Privilege.CREATE_VOLUME,
                FileSpaceType.VOLUME, "media", "other", null));
        assertThatThrownBy(() -> PermissionChecker.check(managed, SESSION, wrongSchema))
                .isInstanceOf(AuthorizationDeniedException.class)
                .hasMessageContaining("VOLUME media.raw.*");

        var writeOnly = accessControl(spaceGrant(Privilege.WRITE_VOLUME,
                FileSpaceType.VOLUME, "media", "raw", "landing"));
        assertThatThrownBy(() -> PermissionChecker.check(alter, SESSION, writeOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("MANAGE_VOLUME");
        assertThatThrownBy(() -> PermissionChecker.check(drop, SESSION, writeOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("MANAGE_VOLUME");

        var manage = accessControl(spaceGrant(Privilege.MANAGE_VOLUME,
                FileSpaceType.VOLUME, "media", "raw", "landing"));
        assertThatCode(() -> PermissionChecker.check(alter, SESSION, manage)).doesNotThrowAnyException();
        assertThatCode(() -> PermissionChecker.check(drop, SESSION, manage)).doesNotThrowAnyException();

        var tableAdmin = accessControl(grant(Privilege.ALL, "*", "*", "*"));
        assertThatThrownBy(() -> PermissionChecker.check(managed, SESSION, tableAdmin))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_VOLUME");
        assertThatThrownBy(() -> PermissionChecker.check(managed, SESSION, null))
                .hasMessageContaining("Authenticated volume or stage principal");
        assertThatThrownBy(() -> PermissionChecker.check(managed, null, manage))
                .hasMessageContaining("Authenticated volume or stage principal");
    }

    @Test
    void stageDdlRequiresItsOwnCreationUsageAndOwnershipPrivileges() {
        var create = new AntlrSqlParser().parse("CREATE STAGE media.raw.stage_landing");
        var refresh = new AntlrSqlParser().parse("ALTER STAGE media.raw.stage_landing REFRESH");
        var drop = new AntlrSqlParser().parse("DROP STAGE media.raw.stage_landing");

        var volumeCreate = accessControl(spaceGrant(Privilege.CREATE_VOLUME,
                FileSpaceType.VOLUME, "media", "raw", null));
        assertThatThrownBy(() -> PermissionChecker.check(create, SESSION, volumeCreate))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_STAGE");

        var stageCreate = accessControl(spaceGrant(Privilege.CREATE_STAGE,
                FileSpaceType.STAGE, "media", "raw", null));
        assertThatCode(() -> PermissionChecker.check(create, SESSION, stageCreate))
                .doesNotThrowAnyException();

        var allStage = accessControl(spaceGrant(Privilege.ALL,
                FileSpaceType.STAGE, "media", "raw", "stage_landing"));
        assertThatThrownBy(() -> PermissionChecker.check(refresh, SESSION, allStage))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("OWNER");
        assertThatThrownBy(() -> PermissionChecker.check(drop, SESSION, allStage))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("OWNER");

        var owner = accessControl(spaceGrant(Privilege.OWNER,
                FileSpaceType.STAGE, "media", "raw", "stage_landing"));
        assertThatCode(() -> PermissionChecker.check(refresh, SESSION, owner)).doesNotThrowAnyException();
        assertThatCode(() -> PermissionChecker.check(drop, SESSION, owner)).doesNotThrowAnyException();

        var wrongStage = accessControl(spaceGrant(Privilege.OWNER,
                FileSpaceType.STAGE, "media", "raw", "other"));
        assertThatThrownBy(() -> PermissionChecker.check(drop, SESSION, wrongStage))
                .isInstanceOf(AuthorizationDeniedException.class)
                .hasMessageContaining("media.raw.stage_landing");
    }

    @Test
    void renameRequiresSourceRenameAndExactDestinationCreatePrivileges() {
        var rename = new AntlrSqlParser().parse("ALTER TABLE secured.source_schema.docs RENAME TO target_schema.renamed");
        var sourceOnly = accessControl(grant(Privilege.RENAME_TABLE, "secured", "source_schema", "docs"));
        assertThatThrownBy(() -> PermissionChecker.check(rename, SESSION, sourceOnly))
                .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("CREATE_TABLE")
                .hasMessageContaining("secured.target_schema.renamed");
        var allowed = accessControl(grant(Privilege.RENAME_TABLE, "secured", "source_schema", "docs"),
                grant(Privilege.CREATE_TABLE, "secured", "target_schema", "renamed"));
        assertThatCode(() -> PermissionChecker.check(rename, SESSION, allowed)).doesNotThrowAnyException();
        var crossCatalog = new AntlrSqlParser().parse("ALTER TABLE secured.source_schema.docs RENAME TO other.target.renamed");
        assertThatThrownBy(() -> PermissionChecker.check(crossCatalog, SESSION, allowed))
                .hasMessageContaining("cannot move a table between catalogs");
    }

    @Test
    void standaloneBranchAndTagDdlRequireAlterOnTheirActualTable() {
        var selectOnly = accessControl(grant(Privilege.SELECT, "secured", "source_schema", "docs"));
        var alter = accessControl(grant(Privilege.ALTER, "secured", "source_schema", "docs"));
        for (String sql : new String[]{"CREATE BRANCH dev ON docs", "DROP BRANCH dev ON docs",
                "CREATE TAG release ON docs", "DROP TAG release ON docs"}) {
            var statement = new AntlrSqlParser().parse(sql);
            assertThatThrownBy(() -> PermissionChecker.check(statement, SESSION, selectOnly))
                    .isInstanceOf(AuthorizationDeniedException.class).hasMessageContaining("ALTER");
            assertThatCode(() -> PermissionChecker.check(statement, SESSION, alter)).doesNotThrowAnyException();
        }
    }

    @Test
    void extraWarmupArgumentsCannotAuthorizeOneTableAndReadAnother() {
        var allowed = accessControl(grant(Privilege.SELECT, "secured", "source_schema", "docs"));
        var malformed = new AntlrSqlParser().parse(
                "CALL secured.warmup_table('source_schema.docs', 'victim', 'ignored')");
        assertThatThrownBy(() -> PermissionChecker.check(malformed, SESSION, allowed))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("warmup_table requires");
    }

    private static QuerySession session(String catalog, String schema) {
        var authenticated = StatementTestSessions.context();
        return new QuerySession(authenticated.queryId(), Map.of("catalog", catalog, "schema", schema),
                authenticated.userIdentity(), authenticated.tenantId(), java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }

    private static PrivilegeGrant grant(Privilege privilege, String catalog, String schema, String table) {
        String objectType = schema == null ? "CATALOG" : table == null ? "SCHEMA" : "TABLE";
        return PrivilegeGrant.of("ddl-role", privilege, objectType, catalog, schema, table, null);
    }

    private static PrivilegeGrant spaceGrant(Privilege privilege, FileSpaceType type,
            String catalog, String schema, String name) {
        return PrivilegeGrant.of("ddl-role", privilege, type.name(), catalog, schema, name, null);
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
