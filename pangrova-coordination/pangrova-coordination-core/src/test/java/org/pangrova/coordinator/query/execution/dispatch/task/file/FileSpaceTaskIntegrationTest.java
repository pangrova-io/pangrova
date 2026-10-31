package org.pangrova.coordinator.query.execution.dispatch.task.file;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.file.core.FileStorageException;
import org.pangrova.file.store.jdbc.space.JdbcFileSpaceRegistry;
import org.pangrova.parser.SqlParser;
import org.pangrova.parser.ast.Statement;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.core.audit.AuditEventEmitter;
import org.pangrova.security.api.audit.AuditEventType;
import org.pangrova.security.core.authz.DynamicRbacAccessControl;
import org.pangrova.security.core.authz.InMemoryRoleStore;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.core.authz.StaticGrantStore;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.core.mask.ColumnMaskingProcessor;
import org.pangrova.security.core.mask.provider.empty.EmptyColumnMaskStore;
import org.pangrova.security.core.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.core.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.store.jdbc.AuthorizationTransactionGate;
import org.pangrova.security.store.jdbc.PersistentGrantStore;
import org.pangrova.security.store.jdbc.PersistentRoleStore;
import org.pangrova.security.api.UserIdentity;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.common.file.FileSpaceType;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real PostgreSQL registry, RBAC checks, audit events, and task dispatch together. */
class FileSpaceTaskIntegrationTest extends ContainerTestBase {

    private static final UserIdentity ALICE =
            new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH);
    private static final String ROLE = "file-admin";

    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();
    private PersistentGrantStore grants;
    private JdbcFileSpaceRegistry registry;
    private MetadataExecutionFactory factory;
    private AuditEventEmitter auditBackend;

    @BeforeEach
    void setUp() throws Exception {
        DataSource source = SharedPostgresContainer.migratedDataSource("file_space_tasks");
        new PersistentRoleStore(source).initSchema();
        try (var connection = source.getConnection()) {
            connection.setAutoCommit(false);
            AuthorizationTransactionGate.initialize(connection);
            connection.commit();
        }
        grants = new PersistentGrantStore(source);
        grants.addGrant(grant(Privilege.CREATE_VOLUME, FileSpaceType.VOLUME, "media", "raw", null));
        grants.addGrant(grant(Privilege.CREATE_EXTERNAL_VOLUME,
                FileSpaceType.VOLUME, "media", "raw", null));
        grants.addGrant(grant(Privilege.READ_VOLUME,
                FileSpaceType.VOLUME, "media", "raw", "landing"));
        grants.addGrant(grant(Privilege.MANAGE_VOLUME,
                FileSpaceType.VOLUME, "media", "raw", "landing"));
        grants.addGrant(grant(Privilege.CREATE_STAGE, FileSpaceType.STAGE, "media", "raw", null));
        grants.addGrant(grant(Privilege.USAGE_STAGE, FileSpaceType.STAGE, "media", "raw", null));
        grants.addGrant(grant(Privilege.OWNER,
                FileSpaceType.STAGE, "media", "raw", "stage_landing"));

        var roles = new InMemoryRoleStore();
        roles.createRole(Role.of(ROLE));
        roles.grantRoleToUser(UserRoleAssignment.of(ALICE.principal(), ROLE));
        var accessControl = new DynamicRbacAccessControl(new StaticGrantStore(
                grants.listGrantsForRole(ROLE)), roles,
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));

        registry = new JdbcFileSpaceRegistry(source);
        auditBackend = new AuditEventEmitter(events::add);
        factory = new MetadataExecutionFactory(Map.of());
        VolumeTasks.register(factory, registry, accessControl,
                new FileSpaceAuditEmitter(auditBackend));
        StageTasks.register(factory, registry, accessControl,
                new FileSpaceAuditEmitter(auditBackend));
    }

    @AfterEach
    void tearDown() {
        if (auditBackend != null) {
            auditBackend.close();
        }
    }

    @Test
    void volumeLifecycleRenamesAndDropsGrantsWithAuditTrail() {
        execute("CREATE VOLUME media.raw.landing COMMENT 'landing zone'");
        assertThat(registry.find(FileSpaceType.VOLUME, "media", "raw", "landing").orElseThrow().owner())
                .isEqualTo("alice");

        StatementResult repeated = execute("CREATE VOLUME IF NOT EXISTS media.raw.landing");
        assertThat(repeated.mutatedTable()).contains("already exists");
        assertThatThrownBy(() -> execute("CREATE VOLUME media.raw.landing"))
                .isInstanceOf(FileStorageException.class);

        createHiddenVolume();
        StatementResult show = execute("SHOW VOLUMES FROM media.raw");
        assertThat(show.rows()).extracting(row -> row.getRaw("name")).containsExactly("landing");
        assertThat(show.rows()).extracting(row -> row.getRaw("type")).containsExactly("MANAGED");
        StatementResult describe = execute("DESCRIBE VOLUME media.raw.landing");
        assertThat(describe.columnNames()).contains("created_at", "updated_at");
        assertThat(describe.rows()).singleElement()
                .satisfies(row -> assertThat(row.getRaw("owner")).isEqualTo("alice"));

        execute("ALTER VOLUME media.raw.landing SET OWNER TO bob");
        assertThat(registry.find(FileSpaceType.VOLUME, "media", "raw", "landing").orElseThrow().owner())
                .isEqualTo("bob");

        execute("ALTER VOLUME media.raw.landing RENAME TO media.raw.landing_v2");
        assertThat(registry.find(FileSpaceType.VOLUME, "media", "raw", "landing")).isEmpty();
        assertThat(registry.find(FileSpaceType.VOLUME, "media", "raw", "landing_v2")).isPresent();
        assertThat(grants.listGrantsForRole(ROLE)).extracting(PrivilegeGrant::table)
                .contains("landing_v2").doesNotContain("landing");

        assertThatThrownBy(() -> execute(
                "ALTER VOLUME media.raw.landing_v2 RENAME TO media.other.landing_v2"))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("cannot move a volume between schemas");

        execute("DROP VOLUME media.raw.landing_v2");
        assertThat(registry.find(FileSpaceType.VOLUME, "media", "raw", "landing_v2")).isEmpty();
        assertThat(grants.listGrantsForRole(ROLE)).extracting(PrivilegeGrant::privilege)
                .doesNotContain(Privilege.MANAGE_VOLUME);
        assertThat(execute("DROP VOLUME IF EXISTS media.raw.landing_v2").mutatedTable())
                .contains("does not exist");

        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> assertThat(events)
                .extracting(AuditEvent::eventType)
                .containsExactly(AuditEventType.VOLUME_CREATED, AuditEventType.VOLUME_ALTERED,
                        AuditEventType.VOLUME_ALTERED, AuditEventType.VOLUME_DROPPED));
        assertThat(events.get(2).properties()).containsEntry("previous_name", "landing");
        assertThat(events.get(1).properties()).containsEntry("previous_owner", "alice");
    }

    @Test
    void stageLifecycleKeepsItsOwnSurfaceAndDefersRefreshToTheFileIndex() {
        execute("CREATE EXTERNAL STAGE IF NOT EXISTS media.raw.stage_landing "
                + "URL = 's3://acme/stage/' STORAGE_PROFILE = 'prod-s3' "
                + "DIRECTORY = (ENABLE = TRUE) COMMENT = 'landing'");
        var created = registry.find(FileSpaceType.STAGE, "media", "raw", "stage_landing")
                .orElseThrow();
        assertThat(created.external()).isTrue();
        assertThat(created.directoryEnabled()).isTrue();
        assertThat(created.location()).isEqualTo("s3://acme/stage");
        assertThat(registry.find(FileSpaceType.VOLUME, "media", "raw", "stage_landing")).isEmpty();

        assertThat(execute("SHOW VOLUMES FROM media.raw").rows()).isEmpty();
        StatementResult show = execute("SHOW STAGES IN media.raw");
        assertThat(show.rows()).extracting(row -> row.getRaw("name"))
                .containsExactly("stage_landing");
        assertThat(show.rows()).extracting(row -> row.getRaw("type"))
                .containsExactly("EXTERNAL");
        assertThat(show.rows()).extracting(row -> row.getRaw("url"))
                .containsExactly("s3://acme/stage");

        execute("ALTER STAGE media.raw.stage_landing SET DIRECTORY = (ENABLE = FALSE)");
        assertThat(registry.find(FileSpaceType.STAGE, "media", "raw", "stage_landing")
                .orElseThrow().directoryEnabled()).isFalse();
        assertThatThrownBy(() -> execute("ALTER STAGE media.raw.stage_landing REFRESH"))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED))
                .hasMessageContaining("directory index");

        execute("DROP STAGE media.raw.stage_landing");
        assertThat(registry.find(FileSpaceType.STAGE, "media", "raw", "stage_landing")).isEmpty();
        assertThat(grants.listGrantsForRole(ROLE)).extracting(PrivilegeGrant::privilege)
                .doesNotContain(Privilege.OWNER);
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> assertThat(events)
                .extracting(AuditEvent::eventType)
                .containsExactly(AuditEventType.STAGE_CREATED, AuditEventType.STAGE_ALTERED,
                        AuditEventType.STAGE_DROPPED));
    }

    private void createHiddenVolume() {
        registry.create(new FileSpaceDefinition(FileSpaceType.VOLUME, "media", "raw", "hidden",
                false, "", "", false, "", "someone-else"), false);
    }

    private StatementResult execute(String sql) {
        Statement statement = new SqlParser().parse(sql);
        return factory.execute(statement, context());
    }

    private static StatementExecutionContext context() {
        CoordinatorSessionContext session = new CoordinatorSessionContext("file-space-query",
                Map.of("catalog", "media", "schema", "raw"), ALICE, "tenant-a");
        return new StatementExecutionContext(null, session.queryId(), session, null, null);
    }

    private static PrivilegeGrant grant(Privilege privilege, FileSpaceType type,
            String catalog, String schema, String name) {
        return PrivilegeGrant.of(ROLE, privilege, type.name(), catalog, schema, name, null);
    }
}
