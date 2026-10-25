package org.pangrova.coordinator.query.statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.control.InMemoryTableControlMaintenanceStore;
import org.pangrova.metadata.control.InMemoryTableControlGovernanceBindingStore;
import org.pangrova.metadata.control.InMemoryTableControlStore;
import org.pangrova.metadata.control.GovernanceBindingType;
import org.pangrova.metadata.control.MaintenanceOperation;
import org.pangrova.metadata.control.OwnershipMode;
import org.pangrova.metadata.control.TableControlMaintenanceService;
import org.pangrova.metadata.control.TableControlPolicyBinder;
import org.pangrova.metadata.control.TableControlService;
import org.pangrova.metadata.control.TableControlSettings;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.schema.SchemaChange;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.metadata.service.MetadataSessionContext;
import org.pangrova.metadata.service.ResolvedTable;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.coordinator.query.audit.TableControlAuditEmitter;
import org.pangrova.parser.SqlParser;
import org.pangrova.security.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.audit.AuditEventType;
import org.pangrova.security.audit.AuditEventWriter;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.pangrova.spi.security.AccessDeniedException;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.coordinator.query.statement.StatementTestSessions.context;

class DdlExecutorOwnershipTest {

    private final SqlParser parser = new SqlParser();
    private MutableMetadata metadata;
    private CatalogRegistry registry;
    private DdlExecutor ddlExecutor;
    private TableControlMaintenanceService maintenanceService;
    private InMemoryTableControlGovernanceBindingStore bindingStore;
    private TableControlPolicyBinder policyBinder;
    private AuditEventEmitter auditEmitter;

    @BeforeEach
    void setUp() {
        metadata = new MutableMetadata();
        metadata.create(new SchemaTableName("default", "users"), "uuid-users");
        ConnectorHandleFactory factory = descriptor ->
                new ConnectorCatalogHandle(descriptor, new TestConnector(metadata));
        registry = new CatalogRegistry(new EmptyMetadataStore(), factory, null, null);
        registry.register(new CatalogDescriptor(
                "test_cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        registry.setTableControlService(new TableControlService(
                new InMemoryTableControlStore(),
                new TableControlSettings(true, true, Set.of())));
        ddlExecutor = new DdlExecutor(registry);
        maintenanceService = new TableControlMaintenanceService(
                new InMemoryTableControlMaintenanceStore());
        ddlExecutor.setMaintenancePolicyService(maintenanceService);
        bindingStore = new InMemoryTableControlGovernanceBindingStore();
        policyBinder = new TableControlPolicyBinder(registry.tableControl(), bindingStore);
        ddlExecutor.setPolicyBinder(policyBinder);
    }

    @AfterEach
    void tearDown() {
        if (auditEmitter != null) {
            auditEmitter.close();
        }
    }

    @Test
    void identityLookupFailureAfterCreateLeavesAnOrphanInsteadOfWritableOwnership() {
        metadata.identityFailure = true;
        CreateTableStatement statement = (CreateTableStatement) parser.parse(
                "CREATE TABLE test_cat.default.orders (id BIGINT)");

        assertThatThrownBy(() -> ddlExecutor.executeCreateTable(statement, context()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("control record could not be registered")
                .hasRootCauseMessage("Identity catalog unavailable");
        assertThat(metadata.tables).containsKey("default.orders");
        assertThat(registry.tableControl().find("test_cat", "default", "orders").orElseThrow().state())
                .isEqualTo(org.pangrova.metadata.control.TableControlState.ORPHANED_MANAGED);
    }

    @Test
    void dropRejectsARecreatedPhysicalTableBeforeDeletingIt() {
        registry.tableControl().registerCreatedFromConnector(metadata, null,
                "test_cat", "default", "users", "alice");
        metadata.create(new SchemaTableName("default", "users"), "uuid-replacement");

        assertThatThrownBy(() -> ddlExecutor.executeDropTable((DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users"), context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("identity");
        assertThat(metadata.tables).containsKey("default.users");
    }

    @Test
    void alterRejectsARecreatedPhysicalTableBeforeChangingIt() {
        registry.tableControl().registerCreatedFromConnector(metadata, null,
                "test_cat", "default", "users", "alice");
        metadata.create(new SchemaTableName("default", "users"), "uuid-replacement");

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADD COLUMN email VARCHAR"), context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("identity");
        assertThat(metadata.altered).isFalse();
    }

    @Test
    void dropRejectsMissingIdentityEvidenceWithoutQuarantiningTheTable() {
        registry.tableControl().registerCreatedFromConnector(metadata, null,
                "test_cat", "default", "users", "alice");
        metadata.identityAvailable = false;

        assertThatThrownBy(() -> ddlExecutor.executeDropTable((DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users"), context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("identity");
        assertThat(metadata.tables).containsKey("default.users");
        assertThat(registry.tableControl().find("test_cat", "default", "users").orElseThrow().state())
                .isEqualTo(org.pangrova.metadata.control.TableControlState.ACTIVE);
    }

    @Test
    void dropOfObservedTableIsRejectedBeforeTheConnectorIsTouched() {
        DropTableStatement stmt = (DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users");

        assertThatThrownBy(() -> ddlExecutor.executeDropTable(stmt, context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");
        assertThat(metadata.tables).containsKey("default.users");
    }

    @Test
    void completedDropCannotRetireOwnershipRegisteredByAConcurrentRecreate() {
        registerManaged("users");
        var original = registry.tableControl().find("test_cat", "default", "users").orElseThrow();
        metadata.afterDrop = () -> {
            registry.tableControl().markDropped(original);
            metadata.create(new SchemaTableName("default", "users"), "replacement");
            registerManaged("users");
        };

        assertThatThrownBy(() -> ddlExecutor.executeDropTable((DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users"), context()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("newer table registration");
        assertThat(metadata.tables).containsKey("default.users");
        assertThat(registry.tableControl().find("test_cat", "default", "users"))
                .hasValueSatisfying(record -> {
                    assertThat(record.tableId()).isNotEqualTo(original.tableId());
                    assertThat(record.formatTableUuid()).isEqualTo("replacement");
                });
    }

    @Test
    void dropOfManagedTableSucceedsAndRetiresOwnership() {
        registerManaged("users");
        DropTableStatement stmt = (DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users");

        ddlExecutor.executeDropTable(stmt, context());

        assertThat(metadata.tables).doesNotContainKey("default.users");
        assertThat(registry.tableControl().find("test_cat", "default", "users")).isEmpty();
    }

    @Test
    void alterOfObservedTableIsRejected() {
        var stmt = parser.parse("ALTER TABLE test_cat.default.users ADD COLUMN email VARCHAR");

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) stmt, context()))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(metadata.altered).isFalse();
    }

    @Test
    void createTableRegistersManagedOwnership() {
        CreateTableStatement stmt = (CreateTableStatement) parser.parse(
                "CREATE TABLE test_cat.default.orders (id BIGINT)");

        ddlExecutor.executeCreateTable(stmt, context());

        assertThat(registry.tableControl().find("test_cat", "default", "orders")).get()
                .satisfies(record -> {
                    assertThat(record.ownershipMode()).isEqualTo(OwnershipMode.MANAGED);
                    assertThat(record.formatTableUuid()).isEqualTo("uuid-orders");
                    assertThat(record.format()).isEqualTo("ICEBERG");
                });
    }

    @Test
    void renameMovesOwnershipWithTheTable() {
        registerManaged("users");

        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users RENAME TO people"),
                context());

        assertThat(registry.tableControl().find("test_cat", "default", "users")).isEmpty();
        assertThat(registry.tableControl().find("test_cat", "default", "people")).isPresent();
        assertThat(metadata.tables).containsKey("default.people");
        var renamed = metadata.getTableHandle(null, new SchemaTableName("default", "people"));
        assertThatCode(() -> registry.tableControl().requireManagedForWrite(
                "test_cat", "default", "people", "INSERT", metadata.getTableIdentity(null, renamed)))
                .doesNotThrowAnyException();
    }

    @Test
    void renameRejectsALiveTargetRecordBeforeTouchingTheCatalog() {
        registerManaged("users");
        metadata.create(new SchemaTableName("default", "people"), "uuid-people");
        registerManaged("people");

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users RENAME TO people"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("already has a live control record");
        assertThat(metadata.tables).containsKey("default.users");
        assertThat(registry.tableControl().find("test_cat", "default", "users")).isPresent();
        assertThat(registry.tableControl().find("test_cat", "default", "people")).isPresent();
    }

    @Test
    void failedRenameRestoresTheCapturedRegistration() {
        registerManaged("users");
        var original = registry.tableControl().find("test_cat", "default", "users").orElseThrow();
        metadata.beforeRename = () -> { throw new IllegalStateException("rename failed"); };

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users RENAME TO people"), context()))
                .isInstanceOf(IllegalStateException.class).hasMessage("rename failed");
        assertThat(registry.tableControl().find("test_cat", "default", "users").orElseThrow().tableId())
                .isEqualTo(original.tableId());
        assertThat(registry.tableControl().find("test_cat", "default", "people")).isEmpty();
    }

    @Test
    void failedRenameCannotRestoreOwnershipOntoAnExternalReplacement() {
        registerManaged("users");
        metadata.beforeRename = () -> {
            metadata.create(new SchemaTableName("default", "users"), "external-replacement");
            throw new IllegalStateException("rename failed");
        };

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users RENAME TO people"), context()))
                .isInstanceOf(IllegalStateException.class).hasMessage("rename failed");
        assertThat(registry.tableControl().find("test_cat", "default", "users")).isEmpty();
        assertThat(registry.tableControl().find("test_cat", "default", "people").orElseThrow().state())
                .isEqualTo(org.pangrova.metadata.control.TableControlState.QUARANTINED);
    }

    @Test
    void controlRegistrationIsSkippedWhenThePlaneIsDisabled() {
        registry.setTableControlService(new TableControlService(
                new InMemoryTableControlStore(),
                new TableControlSettings(false, false, Set.of())));
        DropTableStatement stmt = (DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users");

        ddlExecutor.executeDropTable(stmt, context());

        assertThat(metadata.tables).doesNotContainKey("default.users");
    }

    @Test
    void adoptObservedTableCreatesLinkedRecordAndIsIdempotent() {
        var adopt = (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                "ALTER TABLE test_cat.default.users ADOPT");

        ddlExecutor.executeAlterTable(adopt, context());
        ddlExecutor.executeAlterTable(adopt, context());

        assertThat(registry.tableControl().find("test_cat", "default", "users")).get()
                .satisfies(record -> {
                    assertThat(record.ownershipMode()).isEqualTo(OwnershipMode.LINKED);
                    assertThat(record.formatTableUuid()).isEqualTo("uuid-users");
                    assertThat(record.versionToken()).isEqualTo("uuid-users:snapshot-1");
                });
    }

    @Test
    void adoptRejectsWhenTheFormatIdentityChangedAfterDropAndRecreate() {
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());
        metadata.create(new SchemaTableName("default", "users"), "uuid-users-recreated");

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("identity");
    }

    @Test
    void adoptRejectsWhenTheConnectorCannotReportIdentity() {
        metadata.identityAvailable = false;

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("stable format identity");
        assertThat(registry.tableControl().find("test_cat", "default", "users")).isEmpty();
    }

    @Test
    void adoptWithPropertiesPersistsTheMaintenancePolicyByTableId() {
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT WITH "
                                + "(maintenance_operations='COMPACT', retain_last='4')"),
                context());

        var record = registry.tableControl().find("test_cat", "default", "users").orElseThrow();
        assertThat(maintenanceService.findPolicy(record.tableId())).get()
                .satisfies(policy -> {
                    assertThat(policy.enabledOperations())
                            .containsExactly(MaintenanceOperation.COMPACT);
                    assertThat(policy.retainLast()).isEqualTo(4);
                    assertThat(policy.requireStableExternalVersion()).isTrue();
                });
    }

    @Test
    void adoptIsWrittenToTheAuditTrailWithTheStableTableId() {
        RecordingAuditWriter writer = new RecordingAuditWriter();
        auditEmitter = new AuditEventEmitter(writer);
        ddlExecutor.setTableControlAudit(new TableControlAuditEmitter(auditEmitter));

        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());

        String tableId = registry.tableControl()
                .find("test_cat", "default", "users").orElseThrow().tableId().toString();
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(5))
                .until(() -> writer.events.stream().anyMatch(event ->
                        event.eventType() == AuditEventType.TABLE_ADOPTED
                                && tableId.equals(event.properties().get("table_id"))));
    }

    @Test
    void rejectedAdoptPropertiesLeaveNoControlRecordBehind() {
        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT WITH (retian_last='4')"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("unknown maintenance property");
        assertThat(registry.tableControl().find("test_cat", "default", "users")).isEmpty();
    }

    @Test
    void adoptWithPropertiesFailsWhenPolicyStorageIsNotConfigured() {
        ddlExecutor.setMaintenancePolicyService(null);

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT WITH "
                                + "(maintenance_operations='COMPACT')"),
                context()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("maintenance policy storage is not configured");
        assertThat(registry.tableControl().find("test_cat", "default", "users")).isEmpty();
    }

    @Test
    void unlinkReturnsLinkedTableToObservedWithoutDroppingIt() {
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());

        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users UNLINK"),
                context());

        assertThat(registry.tableControl().find("test_cat", "default", "users")).isEmpty();
        assertThat(metadata.tables).containsKey("default.users");
    }

    @Test
    void unlinkRejectsObservedAndManagedTables() {
        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users UNLINK"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");

        registerManaged("users");
        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users UNLINK"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("MANAGED");
    }

    @Test
    void unlinkPurgesGovernanceBindingsForTheRetiredTable() {
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());
        var tableId = registry.tableControl()
                .find("test_cat", "default", "users").orElseThrow().tableId();
        policyBinder.bind("test_cat", "default", "users",
                GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of(), "alice");

        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users UNLINK"),
                context());

        assertThat(bindingStore.list(tableId)).isEmpty();
    }

    @Test
    void dropPurgesGovernanceBindingsForTheRetiredTable() {
        registerManaged("users");
        var tableId = registry.tableControl()
                .find("test_cat", "default", "users").orElseThrow().tableId();
        policyBinder.bind("test_cat", "default", "users",
                GovernanceBindingType.COLUMN_MASK, "email_mask", Map.of(), "alice");

        ddlExecutor.executeDropTable((DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users"), context());

        assertThat(bindingStore.list(tableId)).isEmpty();
    }

    @Test
    void columnTagRequiresAdoptionAndBindsToTheStableTableId() {
        RecordingMetadataService tags = new RecordingMetadataService();
        ddlExecutor.setMetadataService(tags);
        var set = (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                "ALTER TABLE test_cat.default.users ALTER COLUMN id SET TAG pii_type = 'PII_EMAIL'");
        var unset = (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                "ALTER TABLE test_cat.default.users ALTER COLUMN id UNSET TAG pii_type");

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(set, context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");
        assertThat(tags.tags).isEmpty();

        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());
        var tableId = registry.tableControl()
                .find("test_cat", "default", "users").orElseThrow().tableId();

        ddlExecutor.executeAlterTable(set, context());
        assertThat(tags.tags).containsEntry("test_cat|default|users|id|pii_type", "PII_EMAIL");
        assertThat(bindingStore.list(tableId)).singleElement()
                .satisfies(binding -> {
                    assertThat(binding.bindingType()).isEqualTo(GovernanceBindingType.COLUMN_TAG);
                    assertThat(binding.bindingRef()).isEqualTo("id:pii_type");
                    assertThat(binding.config()).containsEntry("value", "PII_EMAIL");
                });

        ddlExecutor.executeAlterTable(unset, context());
        assertThat(tags.tags).isEmpty();
        assertThat(bindingStore.list(tableId)).isEmpty();
    }

    @Test
    void columnTagRejectsUnknownColumns() {
        RecordingMetadataService tags = new RecordingMetadataService();
        ddlExecutor.setMetadataService(tags);
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ALTER COLUMN missing SET TAG pii_type = 'PII_EMAIL'"),
                context()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("Column not found");
        assertThat(tags.tags).isEmpty();
    }

    @Test
    void columnTagRequiresTheMetadataService() {
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ALTER COLUMN id SET TAG pii_type = 'PII_EMAIL'"),
                context()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("metadata service");
    }

    @Test
    void unlinkClearsDurableColumnTags() {
        RecordingMetadataService tags = new RecordingMetadataService();
        ddlExecutor.setMetadataService(tags);
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ALTER COLUMN id SET TAG pii_type = 'PII_EMAIL'"),
                context());
        assertThat(tags.tags).isNotEmpty();

        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users UNLINK"),
                context());

        assertThat(tags.tags).isEmpty();
    }

    private void registerManaged(String table) {
        registry.tableControl().registerCreatedFromConnector(metadata, null,
                "test_cat", "default", table, "alice");
    }

    private static final class RecordingMetadataService implements MetadataService {
        private final Map<String, String> tags = new LinkedHashMap<>();

        @Override
        public void createCatalog(MetadataSessionContext session, CatalogDescriptor descriptor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropCatalog(MetadataSessionContext session, String catalogName, boolean cascade) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listCatalogs(MetadataSessionContext session) {
            return List.of();
        }

        @Override
        public List<String> listSchemas(MetadataSessionContext session, String catalogName) {
            return List.of();
        }

        @Override
        public ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void createTable(MetadataSessionContext session, QualifiedTableRef tableRef,
                TableMetadata metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef,
                List<SchemaChange> changes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void beginQuery(MetadataSessionContext session) { }

        @Override
        public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) { }

        @Override
        public void setTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                String columnName, String tagKey, String value) {
            tags.put(key(tableRef, columnName, tagKey), value);
        }

        @Override
        public void unsetTag(MetadataSessionContext session, QualifiedTableRef tableRef,
                String columnName, String tagKey) {
            tags.remove(key(tableRef, columnName, tagKey));
        }

        @Override
        public Map<String, String> listTags(MetadataSessionContext session,
                QualifiedTableRef tableRef, String columnName) {
            String prefix = prefix(tableRef, columnName);
            Map<String, String> result = new LinkedHashMap<>();
            tags.forEach((key, value) -> {
                if (key.startsWith(prefix)) {
                    result.put(key.substring(prefix.length()), value);
                }
            });
            return Map.copyOf(result);
        }

        @Override
        public int clearTableTags(MetadataSessionContext session, QualifiedTableRef tableRef) {
            String prefix = tableRef.catalog() + "|" + tableRef.schema() + "|"
                    + tableRef.table() + "|";
            int before = tags.size();
            tags.keySet().removeIf(key -> key.startsWith(prefix));
            return before - tags.size();
        }

        private static String key(QualifiedTableRef ref, String columnName, String tagKey) {
            return prefix(ref, columnName) + tagKey;
        }

        private static String prefix(QualifiedTableRef ref, String columnName) {
            return ref.catalog() + "|" + ref.schema() + "|" + ref.table() + "|"
                    + columnName + "|";
        }
    }

    private static final class TestConnector implements Connector {
        private final ConnectorMetadata metadata;

        TestConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override public ConnectorMetadata getMetadata(ConnectorSession s) { return metadata; }
        @Override public ConnectorSplitManager getSplitManager() { return null; }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { return null; }
    }

    private static final class MutableMetadata implements ConnectorMetadata {

        private final Map<String, ConnectorTableMetadata> tables = new LinkedHashMap<>();
        private final Map<String, String> uuids = new LinkedHashMap<>();
        private boolean altered;
        private boolean identityAvailable = true;
        private boolean identityFailure;
        private Runnable afterDrop = () -> {};
        private Runnable beforeRename = () -> {};

        void create(SchemaTableName name, String uuid) {
            tables.put(key(name), new ConnectorTableMetadata(name, List.of("id"), Map.of()));
            uuids.put(key(name), uuid);
        }

        private static String key(SchemaTableName name) {
            return name.schema() + "." + name.table();
        }

        @Override
        public ConnectorOutputTableHandle createTable(ConnectorSession session,
                ConnectorTableMetadata table) {
            create(table.table(), "uuid-" + table.table().table());
            return null;
        }

        @Override
        public void dropTable(ConnectorSession session, ConnectorTableHandle handle) {
            String key = ((Handle) handle).key;
            tables.remove(key);
            uuids.remove(key);
            afterDrop.run();
        }

        @Override
        public void renameTable(ConnectorSession session, ConnectorTableHandle handle,
                String newSchema, String newTable) {
            beforeRename.run();
            String key = ((Handle) handle).key;
            ConnectorTableMetadata table = tables.remove(key);
            String uuid = uuids.remove(key);
            String newKey = newSchema + "." + newTable;
            tables.put(newKey, new ConnectorTableMetadata(
                    new SchemaTableName(newSchema, newTable), table.columns(), table.properties()));
            uuids.put(newKey, uuid);
        }

        @Override
        public void addColumn(ConnectorSession session, ConnectorTableHandle handle,
                String columnName, org.pangrova.spi.connector.data.type.PangrovaType type,
                Object defaultValue) {
            altered = true;
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return tables.containsKey(key(name)) ? new Handle(key(name)) : null;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                ConnectorTableHandle handle) {
            return tables.get(((Handle) handle).key);
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session,
                ConnectorTableHandle handle) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session,
                ConnectorTableHandle handle) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("default");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return tables.values().stream().map(ConnectorTableMetadata::table).toList();
        }

        @Override
        public Optional<TableIdentity> getTableIdentity(ConnectorSession session,
                ConnectorTableHandle handle) {
            if (identityFailure) {
                throw new IllegalStateException("Identity catalog unavailable");
            }
            if (!identityAvailable) {
                return Optional.empty();
            }
            String key = ((Handle) handle).key;
            return Optional.of(new TableIdentity(
                    "ICEBERG", uuids.get(key), "s3://bucket/" + key, "schema"));
        }

        @Override
        public Optional<TableVersionToken> observeVersion(ConnectorSession session,
                ConnectorTableHandle handle) {
            return identityAvailable
                    ? Optional.of(TableVersionToken.of(
                            "ICEBERG", uuids.get(((Handle) handle).key), "snapshot-1"))
                    : Optional.empty();
        }
    }

    private record Handle(String key) implements ConnectorTableHandle { }

    private static final class RecordingAuditWriter implements AuditEventWriter {
        private final java.util.List<AuditEvent> events =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public void write(AuditEvent event) {
            events.add(event);
        }
    }

    private static final class EmptyMetadataStore implements MetadataStore {
        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) { }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String c, List<String> s) { }
    }
}
