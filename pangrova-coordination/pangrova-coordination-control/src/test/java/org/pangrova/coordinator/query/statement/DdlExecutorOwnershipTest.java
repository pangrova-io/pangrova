package org.pangrova.coordinator.query.statement;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import org.pangrova.metadata.testinfra.TestCatalogRegistry;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.persistence.table.memory.maintenance.InMemoryTableControlMaintenanceStore;
import org.pangrova.persistence.table.memory.policy.InMemoryTablePolicyRepository;
import org.pangrova.persistence.table.memory.control.InMemoryTableControlStore;
import org.pangrova.table.api.enums.GovernanceBindingType;
import org.pangrova.table.api.enums.MaintenanceOperation;
import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.maintenance.TableControlMaintenanceService;
import org.pangrova.table.policy.TableControlPolicyBinder;
import org.pangrova.table.ownership.TableControlService;
import org.pangrova.table.config.TableControlSettings;
import org.pangrova.governance.api.classification.TagChangedEvent;
import org.pangrova.metadata.catalog.CatalogMetadata;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.coordinator.query.audit.TableControlAuditEmitter;
import org.pangrova.parser.parse.AntlrSqlParser;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.api.enums.AuditEventType;
import org.pangrova.security.api.audit.AuditEventWriter;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.connector.api.metadata.table.TableVersionToken;
import org.pangrova.security.api.exception.AccessDeniedException;

import java.util.ArrayList;
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

    private final AntlrSqlParser parser = new AntlrSqlParser();
    private MutableMetadata metadata;
    private TestCatalogRegistry registry;
    private DdlExecutor ddlExecutor;
    private TableControlMaintenanceService maintenanceService;
    private InMemoryTablePolicyRepository bindingStore;
    private TableControlPolicyBinder policyBinder;
    private AuditEventEmitter auditEmitter;

    @BeforeEach
    void setUp() {
        metadata = new MutableMetadata();
        metadata.create(new SchemaTableName("default", "users"), "uuid-users");
        ConnectorHandleFactory factory = descriptor ->
                new ConnectorCatalogHandle(descriptor, new TestConnector(metadata));
        registry = new TestCatalogRegistry(memoryTableStores(), new EmptyCatalogDeployments(), factory, null, null);
        registry.register(new CatalogDescriptor(
                "test_cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        registry.setTableControlService(new TableControlService(
                new InMemoryTableControlStore(), registry::requireCurrentCatalogIdentity,
                new TableControlSettings(true, true, Set.of())));
        ddlExecutor = new DdlExecutor(registry);
        maintenanceService = new TableControlMaintenanceService(
                new InMemoryTableControlMaintenanceStore());
        ddlExecutor.setMaintenancePolicyService(maintenanceService);
        bindingStore = new InMemoryTablePolicyRepository();
        registry.setGovernanceBindingStore(bindingStore);
        policyBinder = registry.tableGovernance();
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
                .isEqualTo(org.pangrova.table.api.enums.TableControlState.ORPHANED_MANAGED);
    }

    @Test
    void replacedCatalogCannotInheritRegistrationOrOrphanFromAnInflightCreate() {
        metadata.afterCreate = () -> {
            registry.unregister("test_cat");
            registry.register(new CatalogDescriptor("test_cat", CatalogType.CONNECTOR, "test",
                    CatalogState.ACTIVE, Map.of(), "", Set.of()));
        };
        var statement = (CreateTableStatement) parser.parse("CREATE TABLE test_cat.default.orders (id BIGINT)");
        assertThatThrownBy(() -> ddlExecutor.executeCreateTable(statement, context()))
                .hasRootCauseMessage("CREATE rejected: catalog deployment changed for test_cat");
        assertThat(registry.tableControl().find("test_cat", "default", "orders")).isEmpty();
    }

    @Test
    void dropRejectsARecreatedPhysicalTableBeforeDeletingIt() {
        registerManaged("users");
        metadata.create(new SchemaTableName("default", "users"), "uuid-replacement");

        assertThatThrownBy(() -> ddlExecutor.executeDropTable((DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users"), context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("identity");
        assertThat(metadata.tables).containsKey("default.users");
    }

    @Test
    void alterRejectsARecreatedPhysicalTableBeforeChangingIt() {
        registerManaged("users");
        metadata.create(new SchemaTableName("default", "users"), "uuid-replacement");

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADD COLUMN email VARCHAR"), context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("identity");
        assertThat(metadata.altered).isFalse();
    }

    @Test
    void dropRejectsMissingIdentityEvidenceWithoutQuarantiningTheTable() {
        registerManaged("users");
        metadata.identityAvailable = false;

        assertThatThrownBy(() -> ddlExecutor.executeDropTable((DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users"), context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("identity");
        assertThat(metadata.tables).containsKey("default.users");
        assertThat(registry.tableControl().find("test_cat", "default", "users").orElseThrow().state())
                .isEqualTo(org.pangrova.table.api.enums.TableControlState.ACTIVE);
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
            registry.tableControl().registrations().markDropped(original);
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
    void shadowDropRejectsARecreatedCatalogBeforeDeletingThePhysicalTable() {
        registry.setTableControlService(new TableControlService(
                new InMemoryTableControlStore(), registry::requireCurrentCatalogIdentity,
                new TableControlSettings(true, false, Set.of())));
        registerManaged("users");
        var captured = registry.tableControl().find("test_cat", "default", "users").orElseThrow();
        registry.unregister("test_cat");
        registry.register(new CatalogDescriptor("test_cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));

        assertThatThrownBy(() -> ddlExecutor.executeDropTable((DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users"), context()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
        assertThat(metadata.tables).containsKey("default.users");
        assertThat(registry.tableControl().find("test_cat", "default", "users")).contains(captured);
    }

    @Test
    void uncertainDropResultDoesNotRetireTheCapturedTableControlRecord() {
        registerManaged("users");
        var original = registry.tableControl().find("test_cat", "default", "users").orElseThrow();
        metadata.beforeDrop = () -> {
            // The table disappeared independently before this connector call could drop it.
            metadata.tables.remove("default.users");
            metadata.uuids.remove("default.users");
            throw new org.pangrova.connector.api.exception.ConnectorException(
                    org.pangrova.common.enums.PangrovaErrorCode.CONNECTOR_DDL_FAILED, "unestablished drop");
        };
        var stmt = (DropTableStatement) parser.parse("DROP TABLE IF EXISTS test_cat.default.users");

        assertThatThrownBy(() -> ddlExecutor.executeDropTable(stmt, context()))
                .isInstanceOf(org.pangrova.connector.api.exception.ConnectorException.class);
        assertThat(registry.tableControl().find("test_cat", "default", "users")).contains(original);
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
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) stmt, context()))
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
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
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
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users RENAME TO people"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("already has a live control record");
        assertThat(metadata.tables).containsKey("default.users");
        assertThat(registry.tableControl().find("test_cat", "default", "users")).isPresent();
        assertThat(registry.tableControl().find("test_cat", "default", "people")).isPresent();
    }

    @Test
    void uncertainRenameKeepsBothNamesReserved() {
        registerManaged("users");
        var original = registry.tableControl().find("test_cat", "default", "users").orElseThrow();
        metadata.beforeRename = () -> { throw new IllegalStateException("rename failed"); };

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users RENAME TO people"), context()))
                .isInstanceOf(IllegalStateException.class).hasMessage("rename failed");
        assertThat(registry.tableControl().find("test_cat", "default", "users").orElseThrow().tableId())
                .isEqualTo(original.tableId());
        assertThat(registry.tableControl().find("test_cat", "default", "people").orElseThrow().state())
                .isEqualTo(org.pangrova.table.api.enums.TableControlState.RENAMING);
        assertThatThrownBy(() -> registry.tableControl().registrations().requireCreatableName("test_cat", "default", "users"))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> registry.tableGovernance().requireBindable("test_cat", "default", "users"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void failedRenameCannotRestoreOwnershipOntoAnExternalReplacement() {
        registerManaged("users");
        metadata.beforeRename = () -> {
            metadata.create(new SchemaTableName("default", "users"), "external-replacement");
            throw new IllegalStateException("rename failed");
        };

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users RENAME TO people"), context()))
                .isInstanceOf(IllegalStateException.class).hasMessage("rename failed");
        assertThat(registry.tableControl().find("test_cat", "default", "users").orElseThrow().state())
                .isEqualTo(org.pangrova.table.api.enums.TableControlState.RENAMING);
        assertThat(registry.tableControl().find("test_cat", "default", "people").orElseThrow().state())
                .isEqualTo(org.pangrova.table.api.enums.TableControlState.RENAMING);
    }

    @Test
    void controlRegistrationIsSkippedWhenThePlaneIsDisabled() {
        registry.setTableControlService(new TableControlService(
                new InMemoryTableControlStore(), registry::requireCurrentCatalogIdentity,
                new TableControlSettings(false, false, Set.of())));
        DropTableStatement stmt = (DropTableStatement) parser.parse(
                "DROP TABLE test_cat.default.users");

        ddlExecutor.executeDropTable(stmt, context());

        assertThat(metadata.tables).doesNotContainKey("default.users");
    }

    @Test
    void adoptObservedTableCreatesLinkedRecordAndIsIdempotent() {
        var adopt = (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
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
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());
        metadata.create(new SchemaTableName("default", "users"), "uuid-users-recreated");

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("identity");
    }

    @Test
    void adoptRejectsWhenTheConnectorCannotReportIdentity() {
        metadata.identityAvailable = false;

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("stable format identity");
        assertThat(registry.tableControl().find("test_cat", "default", "users")).isEmpty();
    }

    @Test
    void adoptWithPropertiesPersistsTheMaintenancePolicyByTableId() {
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
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
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
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
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
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
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
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
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());

        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users UNLINK"),
                context());

        assertThat(registry.tableControl().find("test_cat", "default", "users")).isEmpty();
        assertThat(metadata.tables).containsKey("default.users");
    }

    @Test
    void unlinkRejectsObservedAndManagedTables() {
        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users UNLINK"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");

        registerManaged("users");
        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users UNLINK"),
                context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("MANAGED");
    }

    @Test
    void unlinkPurgesGovernanceBindingsForTheRetiredTable() {
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());
        var tableId = registry.tableControl()
                .find("test_cat", "default", "users").orElseThrow().tableId();
        policyBinder.bind("test_cat", "default", "users",
                GovernanceBindingType.COLUMN_TAG, "classification", Map.of(), "alice");

        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
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
        List<TagChangedEvent> events = observeColumnTagEvents();
        var set = (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                "ALTER TABLE test_cat.default.users ALTER COLUMN id SET TAG classification = 'sensitive'");
        var unset = (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                "ALTER TABLE test_cat.default.users ALTER COLUMN id UNSET TAG classification");

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(set, context()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");
        assertThat(events).isEmpty();

        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());
        var tableId = registry.tableControl()
                .find("test_cat", "default", "users").orElseThrow().tableId();

        ddlExecutor.executeAlterTable(set, context());
        assertThat(events).containsExactly(new TagChangedEvent(
                "test_cat", "default", "users", "id", "classification", null, "sensitive", tableId));
        assertThat(bindingStore.list(tableId)).singleElement()
                .satisfies(binding -> {
                    assertThat(binding.bindingType()).isEqualTo(GovernanceBindingType.COLUMN_TAG);
                    assertThat(binding.bindingRef()).isEqualTo("2:idclassification");
                    assertThat(binding.config()).containsEntry("value", "sensitive");
                });

        ddlExecutor.executeAlterTable(unset, context());
        assertThat(events).hasSize(2);
        assertThat(events.get(1)).isEqualTo(new TagChangedEvent(
                "test_cat", "default", "users", "id", "classification", "sensitive", null, tableId));
        assertThat(bindingStore.list(tableId)).isEmpty();
    }

    @Test
    void columnTagRejectsUnknownColumns() {
        List<TagChangedEvent> events = observeColumnTagEvents();
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ALTER COLUMN missing SET TAG classification = 'sensitive'"),
                context()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("Column not found");
        assertThat(events).isEmpty();
    }

    @Test
    void columnTagRequiresTheMetadataService() {
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());

        assertThatThrownBy(() -> ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ALTER COLUMN id SET TAG classification = 'sensitive'"),
                context()))
                .isInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                .hasMessageContaining("metadata service");
    }

    @Test
    void unlinkClearsDurableColumnTags() {
        List<TagChangedEvent> events = observeColumnTagEvents();
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ADOPT"),
                context());
        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users ALTER COLUMN id SET TAG classification = 'sensitive'"),
                context());
        var tableId = registry.tableControl().find("test_cat", "default", "users").orElseThrow().tableId();
        assertThat(bindingStore.list(tableId)).hasSize(1);
        assertThat(events).hasSize(1);

        ddlExecutor.executeAlterTable(
                (org.pangrova.parser.api.ast.statement.AlterTableStatement) parser.parse(
                        "ALTER TABLE test_cat.default.users UNLINK"),
                context());

        assertThat(bindingStore.list(tableId)).isEmpty();
        assertThat(events).hasSize(2);
        assertThat(events.get(1)).isEqualTo(new TagChangedEvent(
                "test_cat", "default", "users", "id", "classification", "sensitive", null, tableId));
    }

    private void registerManaged(String table) {
        registry.tableControl().registrations().registerCreatedFromConnector(metadata, null,
                "test_cat", "default", table, "alice", registry.requireCurrentCatalogIdentity("test_cat"));
    }

    private List<TagChangedEvent> observeColumnTagEvents() {
        var catalog = new CatalogMetadata(registry, new TieredMetadataCache(), null, new SchemaEvolutionValidator());
        List<TagChangedEvent> events = new ArrayList<>();
        catalog.columnTags().columnTagEvents().addListener(events::add);
        ddlExecutor.setMetadataService(catalog);
        return events;
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
        private Runnable afterCreate = () -> {};
        private Runnable afterDrop = () -> {};
        private Runnable beforeDrop = () -> {};
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
            afterCreate.run();
            return null;
        }

        @Override
        public void dropTable(ConnectorSession session, ConnectorTableHandle handle) {
            beforeDrop.run();
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
                String columnName, org.pangrova.types.enums.PangrovaType type,
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

    private static final class EmptyCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        private final Map<String, CatalogDescriptor> descriptors = new LinkedHashMap<>();

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { descriptors.put(d.name(), d); }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.ofNullable(descriptors.get(n)); }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { return descriptors.entrySet().removeIf(entry -> entry.getValue().identity().equals(expected)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }

    }
}
