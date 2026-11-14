package org.pangrova.coordinator.query.execution;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.query.api.write.WriteAuthorityRequest;

import org.pangrova.query.api.enums.QueryState;

import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.nio.file.Path;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.connector.iceberg.handle.IcebergOutputHandleCodec;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.query.task.write.NativeWriteCommitTracker;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.security.policy.builtin.authorization.DynamicRbacAccessControl;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.testing.testkit.ContainerTestBase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies DML cannot be dispatched without a registered commit authority. */
class QuerySchedulingEngineWriteAdmissionTest extends ContainerTestBase {
    @TempDir
    Path warehouse;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void ordinaryAndGeneratedWritesFailBeforeSdkAllocationWithoutAuthority(boolean generated) throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, generated)) {
            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(
                    session("missing-authority"), fixture.context()))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("before connector allocation");

            assertNoAllocation(fixture, "missing-authority");
        }
    }

    @Test
    void observedTableIsRejectedBeforeSdkAllocationWhenOwnershipIsEnforced() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.enforceOwnership();
            fixture.installOrdinaryAuthority();

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(
                    session("observed-write"), fixture.context()))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("OBSERVED");

            assertNoAllocation(fixture, "observed-write");
            assertThat(fixture.observations).isEmpty();
        }
    }

    @Test
    void undeclaredWriteCapabilityIsRejectedBeforeSdkAllocation() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            fixture.metadata.capabilities =
                    org.pangrova.connector.api.metadata.table.TableCapabilities.UNSUPPORTED;

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(
                    session("undeclared-write"), fixture.context()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasRootCauseInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                    .hasRootCauseMessage("CountingMetadata does not support writes on "
                            + "catalog.schema.documents (no write capability declared)");

            assertNoAllocation(fixture, "undeclared-write");
            assertThat(fixture.observations).isEmpty();
        }
    }

    @Test
    void undeclaredDeleteCapabilityIsRejectedBeforeSdkAllocation() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            fixture.metadata.capabilities =
                    new org.pangrova.connector.api.metadata.table.TableCapabilities(
                            true, false, false, true, true, true, Set.of(), Set.of(),
                            org.pangrova.connector.api.metadata.table.TableCapabilities.WriterFenceKind
                                    .FORMAT_IDENTITY);
            var context = new DmlWriteContext(
                    new QualifiedTableName("catalog", "schema", "documents"), "DELETE", List.of(),
                    Optional.of(new WritePolicyObservation(
                            List.of(), Set.of(Privilege.DELETE), fixture.declaration())));

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(
                    session("undeclared-delete"), context))
                    .isInstanceOf(IllegalStateException.class)
                    .hasRootCauseInstanceOf(org.pangrova.common.exception.PangrovaException.class)
                    .hasRootCauseMessage("CountingMetadata does not support DELETE on "
                            + "catalog.schema.documents (no delete capability declared)");

            assertNoAllocation(fixture, "undeclared-delete");
        }
    }

    @Test
    void aRecreatedTableIsQuarantinedBeforeSdkAllocation() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.enforceOwnership();
            fixture.installOrdinaryAuthority();
            fixture.registry.tableControl().registrations().registerCreated(
                    "catalog", "schema", "documents", "alice",
                    Optional.of(new org.pangrova.connector.api.metadata.table.TableIdentity(
                            "ICEBERG", "stale-uuid", "s3://stale", "schema")),
                    Optional.empty());

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(
                    session("recreated-write"), fixture.context()))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("no longer matches");

            assertNoAllocation(fixture, "recreated-write");
            assertThat(fixture.registry.tableControl().find("catalog", "schema", "documents")).get()
                    .extracting(org.pangrova.table.api.ownership.TableControlRecord::state)
                    .isEqualTo(org.pangrova.table.api.enums.TableControlState.QUARANTINED);
        }
    }

    @Test
    void managedTableStillAllocatesWhenOwnershipIsEnforced() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.enforceOwnership();
            fixture.installOrdinaryAuthority();
            fixture.registry.tableControl().registrations().registerCreatedFromConnector(fixture.metadata,
                    org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(
                            session("managed-write"), "catalog", "schema"),
                    "catalog", "schema", "documents", "alice", fixture.registry.requireCurrentCatalogIdentity("catalog"));

            fixture.engine.ensureWriteContextRegistered(session("managed-write"), fixture.context());

            assertThat(fixture.metadata.inserts).isEqualTo(1);
            assertThat(fixture.tracker.hasWriteContext("managed-write")).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void currentPostgresRevocationBlocksOrdinaryAndGeneratedAllocation(boolean generated) throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("write_admission");
        var roles = new JdbcRoleStore(dataSource);
        roles.createRole(Role.of("writer"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "writer"));
        var grants = new JdbcGrantStore(dataSource);
        grants.addGrant(PrivilegeGrant.of("writer", Privilege.INSERT, "TABLE", "catalog", "schema", "documents", null));
        var authority = new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(new JdbcRowPolicyStore(dataSource)),
                new ColumnMaskingProcessor(new JdbcColumnMaskStore(dataSource)));
        authority.checkCanInsertIntoTable(session("revoked").userIdentity(), "catalog", "schema", "documents");
        grants.revokeGrants("writer", Privilege.INSERT, "catalog", "schema", "documents");

        try (var fixture = new SchedulingWriteFixture(warehouse, generated)) {
            fixture.createEngineWithAuthority(request -> {
                fixture.observations.add(request);
                authority.checkCanInsertIntoTable(session("revoked").userIdentity(),
                        request.table().catalog(), request.table().schema(), request.table().table());
                throw new AssertionError("Revoked PostgreSQL grant must fail before receipt creation");
            });
            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(
                    session("revoked"), fixture.context()))
                    .isInstanceOf(AccessDeniedException.class);
            assertThat(fixture.observations).hasSize(1);
            assertNoAllocation(fixture, "revoked");
        }
    }

    @Test
    void missingCompilationEvidenceDoesNotEvenObserveAuthorityOrAllocate() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            var uncompiled = new DmlWriteContext(
                    new QualifiedTableName("catalog", "schema", "documents"), "INSERT", List.of(), Optional.empty());

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(session("uncompiled"), uncompiled))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("compiled policy observations");
            assertThat(fixture.observations).isEmpty();
            assertNoAllocation(fixture, "uncompiled");
        }
    }

    @Test
    void aReceiptForAnotherTableCannotAllocateTheActualSdkTable() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.createEngineWithAuthority(request ->
                    fixture.receipt(requestForOtherTable(request)));

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(
                    session("wrong-declaration"), fixture.context()))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("current table declaration");
            assertNoAllocation(fixture, "wrong-declaration");
        }
    }

    @ParameterizedTest
    @CsvSource({"INSERT, INSERT, INSERT", "DELETE, DELETE, OVERWRITE", "UPDATE, UPDATE, OVERWRITE", "MERGE, UPDATE, OVERWRITE"})
    void soleAllocationPreservesOriginalActionAndExactSdkOutput(String mode, Privilege privilege,
            String allocationMode) throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            var observation = new WritePolicyObservation(List.of(), Set.of(privilege), fixture.declaration());
            fixture.engine.ensureWriteContextRegistered(session("allocated"),
                    new DmlWriteContext(
                    new QualifiedTableName("catalog", "schema", "documents"), mode, List.of(), Optional.of(observation)));

            assertThat(fixture.observations).singleElement().satisfies(request -> {
                assertThat(request.writeMode()).isEqualTo(mode);
                assertThat(request.requiredWritePrivileges()).containsExactly(privilege);
                assertThat(request.observedReadPermissions()).isEmpty();
                assertThat(request.session().getUser()).isEqualTo("alice");
                assertThat(request.session().getProperties()).containsEntry("tenant_id", "tenant-1");
            });
            var state = fixture.tracker.peekWriteState("allocated");
            assertThat(state).isNotNull();
            var binding = WriteOutputBinding.parseFrom(state.writeOutputBinding());
            var wire = binding.getConnector().getOutputHandle();
            var decoded = new IcebergOutputHandleCodec().deserialize(wire.getCodecVersion(), wire.getPayload().toByteArray());
            assertThat(decoded).isEqualTo(state.outputHandle());
            assertThat((IcebergOutputTableHandle) decoded).extracting(IcebergOutputTableHandle::writeMode).isEqualTo(allocationMode);
            assertThat(wire.getWriteColumnNamesList()).containsExactly("id", "title", "embedding");
            assertThat(fixture.metadata.inserts).isEqualTo(mode.equals("INSERT") ? 1 : 0);
            assertThat(fixture.metadata.overwrites).isEqualTo(mode.equals("INSERT") ? 0 : 1);
        }
    }

    @Test
    void staticOverwritePassesEveryExactPartitionBindingToTheConnector() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            var context = new DmlWriteContext(
                    new QualifiedTableName("catalog", "schema", "documents"), "OVERWRITE",
                    List.of("region=eu=west", "day=2026-10-01", "empty="),
                    Optional.of(new WritePolicyObservation(List.of(), Set.of(Privilege.INSERT), fixture.declaration())));

            fixture.engine.ensureWriteContextRegistered(session("partitioned"), context);

            assertThat(fixture.metadata.partition).containsExactlyInAnyOrderEntriesOf(
                    Map.of("region", "eu=west", "day", "2026-10-01", "empty", ""));
            assertThat(fixture.metadata.partitionOverwrites).isEqualTo(1);
            assertThat(fixture.metadata.overwrites).isEqualTo(1);
            assertThat(fixture.tracker.hasWriteContext("partitioned")).isTrue();
        }
    }

    @Test
    void actualCompiledSourcePoliciesArePassedUnchangedToTheSoleAuthoritySupplier() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            var actor = session("source-observation");
            var observation = fixture.compileProtectedInsert(actor);
            fixture.installOrdinaryAuthority();

            fixture.engine.ensureWriteContextRegistered(actor, new DmlWriteContext(
                    new QualifiedTableName("catalog", "schema", "documents"), "INSERT",
                    List.of(), Optional.of(observation)));

            assertThat(fixture.observations).singleElement().satisfies(request -> {
                assertThat(request.observedReadPermissions()).isEqualTo(observation.reads()).singleElement()
                        .satisfies(read -> {
                            assertThat(read.table().toString()).isEqualTo("catalog.schema.other");
                            assertThat(read.columns()).containsExactlyInAnyOrder("id", "title", "embedding");
                            assertThat(read.observedPolicy().rowFilter()).contains("(id > 0)");
                            assertThat(read.observedPolicy().columnMasks()).containsEntry("title", Optional.of("'masked'"))
                                    .containsEntry("id", Optional.empty()).containsEntry("embedding", Optional.empty());
                        });
                assertThat(request.requiredWritePrivileges()).containsExactly(Privilege.INSERT);
            });
            assertThat(fixture.metadata.inserts).isEqualTo(1);
            assertThat(fixture.tracker.hasWriteContext("source-observation")).isTrue();
        }
    }

    @Test
    void failedBeginOverwriteDoesNotRegisterACommitOrReachDispatch() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            fixture.metadata.failOverwrite = true;
            var context = new DmlWriteContext(
                    new QualifiedTableName("catalog", "schema", "documents"), "DELETE", List.of(),
                    Optional.of(new WritePolicyObservation(List.of(), Set.of(Privilege.DELETE), fixture.declaration())));

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(session("failed-overwrite"), context))
                    .isInstanceOf(IllegalStateException.class).rootCause()
                    .hasMessageContaining("allocation boundary rejected overwrite");
            assertThat(fixture.metadata.overwrites).isEqualTo(1);
            assertThat(fixture.metadata.inserts).isZero();
            assertThat(fixture.tracker.hasWriteContext("failed-overwrite")).isFalse();
        }
    }

    @Test
    void duplicateRegistrationReobservesAuthorityButDoesNotReallocate() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            var context = fixture.context();
            fixture.engine.ensureWriteContextRegistered(session("duplicate"), context);
            var original = fixture.tracker.peekWriteState("duplicate");

            fixture.engine.ensureWriteContextRegistered(session("duplicate"), context);

            assertThat(fixture.observations).hasSize(2);
            assertThat(fixture.metadata.inserts).isEqualTo(1);
            assertThat(fixture.tracker.peekWriteState("duplicate")).isSameAs(original);
        }
    }

    @Test
    void duplicateQueryCannotChangeItsOriginalWritePrivilege() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            fixture.engine.ensureWriteContextRegistered(session("changed"), fixture.context());
            var original = fixture.tracker.peekWriteState("changed");
            var changed = new DmlWriteContext(
                    new QualifiedTableName("catalog", "schema", "documents"), "DELETE", List.of(),
                    Optional.of(new WritePolicyObservation(List.of(), Set.of(Privilege.DELETE), fixture.declaration())));

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(session("changed"), changed))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("different authority or declaration");
            assertThat(fixture.observations).hasSize(2);
            assertThat(fixture.metadata.inserts).isEqualTo(1);
            assertThat(fixture.metadata.overwrites).isZero();
            assertThat(fixture.tracker.peekWriteState("changed")).isSameAs(original);
        }
    }

    @Test
    void duplicateQueryMustRejectAnActualSdkSchemaChangeBeforeReallocation() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            var context = fixture.context();
            fixture.engine.ensureWriteContextRegistered(session("schema-change"), context);
            var original = fixture.tracker.peekWriteState("schema-change");
            fixture.evolveActualSchema();

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(session("schema-change"), context))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("changed before admission");
            assertThat(fixture.observations).hasSize(1);
            assertThat(fixture.metadata.inserts).isEqualTo(1);
            assertThat(fixture.tracker.peekWriteState("schema-change")).isSameAs(original);
        }
    }

    @ParameterizedTest
    @CsvSource({"alice, tenant-2", "bob, tenant-1"})
    void duplicateQueryCannotReplaceItsAuthenticatedPrincipalOrTenant(String principal, String tenant) throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            var context = fixture.context();
            fixture.engine.ensureWriteContextRegistered(session("identity-change"), context);
            var original = fixture.tracker.peekWriteState("identity-change");
            var changed = new QuerySession("identity-change", Map.of(),
                    new UserIdentity(principal, Set.of(), Map.of(), Instant.EPOCH), tenant, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(changed, context))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("different authority or declaration");
            assertThat(fixture.observations).hasSize(2);
            assertThat(fixture.metadata.inserts).isEqualTo(1);
            assertThat(fixture.tracker.peekWriteState("identity-change")).isSameAs(original);
        }
    }

    @Test
    void duplicateStaticPartitionNamesFailBeforeCallingTheConnector() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            fixture.installOrdinaryAuthority();
            var context = new DmlWriteContext(
                    new QualifiedTableName("catalog", "schema", "documents"), "OVERWRITE", List.of("day=a", "day=b"),
                    Optional.of(new WritePolicyObservation(List.of(), Set.of(Privilege.INSERT), fixture.declaration())));

            assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(session("duplicate-partition"), context))
                    .isInstanceOf(IllegalStateException.class).rootCause().isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unique name=value pairs");
            assertThat(fixture.metadata.partitionOverwrites).isZero();
            assertNoAllocation(fixture, "duplicate-partition");
        }
    }

    @Test
    void writeModeRejectsUnknownAndLegacyAliasesBeforeDispatch() {
        for (String mode : List.of("INSERT", "OVERWRITE", "UPDATE", "DELETE", "MERGE")) {
            assertThatCode(() -> new DmlWriteContext(
                    new QualifiedTableName("catalog", "schema", "table"),
                    mode, List.of(), java.util.Optional.empty())).doesNotThrowAnyException();
        }
        for (String mode : List.of("APPEND", "UPSERT", "", "insert")) {
            assertThatThrownBy(() -> new DmlWriteContext(
                    new QualifiedTableName("catalog", "schema", "table"),
                    mode, List.of(), java.util.Optional.empty()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported distributed write mode");
        }
    }

    @Test
    void missingCommitTrackerFailsBeforeWorkerDispatch() {
        QuerySchedulingEngine engine = engine(null);

        assertThatThrownBy(() -> engine.ensureWriteContextRegistered(
                session("query-1"), context(new QualifiedTableName("catalog", "schema", "table"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("commit tracker");
    }

    @Test
    void unresolvedTenantFailsBeforeWorkerDispatch() {
        QuerySchedulingEngine engine = engine(new CatalogRegistry(memoryTableStores(), null));
        engine.setWriteCommitTracker(new NativeWriteCommitTracker());

        assertThatThrownBy(() -> engine.ensureWriteContextRegistered(
                new QuerySession("query-no-tenant", java.util.Map.of(), null, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture()),
                context(new QualifiedTableName("catalog", "schema", "table"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("resolved tenant");
    }

    @Test
    void malformedTargetFailsBeforeWorkerDispatch() {
        QuerySchedulingEngine engine = engine(new CatalogRegistry(memoryTableStores(), null));
        engine.setWriteCommitTracker(new NativeWriteCommitTracker());

        assertThatThrownBy(() -> engine.ensureWriteContextRegistered(
                session("query-2"), context(new QualifiedTableName("", "schema", "table"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("catalog.schema.table");
    }

    @Test
    void missingCatalogFailsBeforeWorkerDispatch() {
        QuerySchedulingEngine engine = engine(new CatalogRegistry(memoryTableStores(), null));
        engine.setWriteCommitTracker(new NativeWriteCommitTracker());

        assertThatThrownBy(() -> engine.ensureWriteContextRegistered(
                session("query-3"), context(new QualifiedTableName("catalog", "schema", "table"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("context registration failed")
                .cause()
                .hasMessageContaining("not found");
    }

    @Test
    void nativeWriteReadinessDoesNotPublishAnUnreadableFlightEndpoint() {
        QueryStateMachine write = schedulingState();
        ResultEndpoint nativeEndpoint = new ResultEndpoint(
                "worker", 9000, "write-task", "arrow-flight");
        DispatchQueryContext writeContext = new DispatchQueryContext(
                Map.of(), "INSERT INTO catalog.schema.table VALUES (1)", null)
                .withDmlWriteContext(context(new QualifiedTableName("catalog", "schema", "table")));

        QuerySchedulingEngine.onTaskReady(write, writeContext, nativeEndpoint);

        assertThat(write.getState()).isEqualTo(QueryState.RUNNING);
        assertThat(write.getResultEndpoint()).isEmpty();
    }

    @Test
    void readTaskStillPublishesItsFlightEndpoint() {
        QueryStateMachine read = schedulingState();
        ResultEndpoint nativeEndpoint = new ResultEndpoint(
                "worker", 9000, "read-task", "arrow-flight");

        QuerySchedulingEngine.onTaskReady(read,
                new DispatchQueryContext(Map.of(), "SELECT 1", null), nativeEndpoint);

        assertThat(read.getResultEndpoint()).contains(nativeEndpoint);
    }

    private static QueryStateMachine schedulingState() {
        QueryStateMachine state = new QueryStateMachine();
        state.tryTransition(QueryState.PLANNING);
        state.tryTransition(QueryState.SCHEDULING);
        return state;
    }

    private static QuerySchedulingEngine engine(CatalogRegistry catalogs) {
        return new QuerySchedulingEngine(new QuerySchedulingEngine.SchedulingDependencies(
                null, null, null, null, catalogs, null, null, 0), null);
    }

    private static DmlWriteContext context(QualifiedTableName table) {
        return new DmlWriteContext(table,
                "INSERT", List.of(), java.util.Optional.of(
                new WritePolicyObservation(List.of(), Set.of(Privilege.INSERT),
                        new org.pangrova.connector.api.commit.WriteDeclarationSnapshot(SchedulingWriteFixture.TABLE,
                                "a".repeat(64), "b".repeat(64), false))));
    }

    private static WriteAuthorityRequest requestForOtherTable(
            WriteAuthorityRequest request) {
        var other = new QualifiedTableName("catalog", "schema", "other");
        var declaration = request.compiledDeclaration();
        return new WriteAuthorityRequest(other, request.session(), request.writeMode(),
                request.observedReadPermissions(), request.requiredWritePrivileges(),
                new org.pangrova.connector.api.commit.WriteDeclarationSnapshot(other, declaration.schemaSha256(),
                        declaration.generatedColumnsSha256(), declaration.hasGeneratedColumns()),
                request.internalMaterializationExecution());
    }

    private static QuerySession session(String queryId) {
        return new QuerySession(queryId, Map.of(),
                new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH), "tenant-1", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }

    private static void assertNoAllocation(SchedulingWriteFixture fixture, String queryId) {
        assertThat(fixture.metadata.inserts).isZero();
        assertThat(fixture.metadata.overwrites).isZero();
        assertThat(fixture.tracker.hasWriteContext(queryId)).isFalse();
    }
}
