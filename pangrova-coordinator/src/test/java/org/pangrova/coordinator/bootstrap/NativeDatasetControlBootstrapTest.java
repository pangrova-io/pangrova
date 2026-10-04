package org.pangrova.coordinator.bootstrap;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.observability.noop.NoOpTracingProvider;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.CatalogDeploymentFingerprint;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintResult;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.FinalizeDistributedWriteRequest;
import org.pangrova.protocol.v2.FinalizeDistributedWriteResult;
import org.pangrova.protocol.v2.LanceWriteBinding;
import org.pangrova.protocol.v2.LanceWriteFragment;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.WriteOutputBinding;
import org.pangrova.protocol.v2.WritePublicationOutcome;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;
import org.pangrova.spi.connector.metadata.NativeWriteAllocation;
import org.pangrova.spi.connector.metadata.NativeWriteFinalization;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.coordination.ExecutionAnchor;
import org.pangrova.spi.coordination.WriteCommitEntry;
import org.pangrova.spi.coordination.WriteCommitFingerprint;
import org.pangrova.testing.ContainerTestBase;

import java.net.HttpURLConnection;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real process services, durable PostgreSQL journal and binary loopback transport. */
class NativeDatasetControlBootstrapTest extends ContainerTestBase {

    private static final List<ConnectorTableMetadata.Column> COLUMNS =
            List.of(new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT));

    @Test
    void lateBindingPublicationAndCloseFollowRealServiceLifecycle() throws Exception {
        var queryRef = new AtomicReference<QueryBootstrap>();
        var ownershipRef = new AtomicReference<HighAvailabilityCoordinationService>();
        var requestRef = new AtomicReference<FinalizeDistributedWriteRequest>();
        var responseOverride = new AtomicReference<RuntimeCommandResponse>();
        var status = new AtomicInteger(HttpURLConnection.HTTP_OK);
        var catalog = catalog();
        var server = server(requestRef, responseOverride, status);
        var queries = new QueryBootstrap(CoordinatorBootstrapConfig.withDefaults("control-owner", 0),
                new NoOpTracingProvider(), null, null, null, new ModuleIntegrationBootstrap(),
                SharedPostgresContainer.migratedDataSource("native_control_lifecycle"));
        try (var ownership = new HighAvailabilityCoordinationService(new InMemoryDistributedLease(),
                    new HighAvailabilityCoordinationService.CoordinationConfig(60_000, 20_000));
             var bootstrap = new NativeDatasetControlBootstrap("credential", queryRef::get, ownershipRef::get)) {
            NativeDatasetControlPlane plane = bootstrap.forCatalog(catalog).orElseThrow();
            assertThatThrownBy(() -> plane.schemaFingerprint(session(), COLUMNS))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not started");
            queryRef.set(queries);
            assertThatThrownBy(() -> plane.schemaFingerprint(session(), COLUMNS))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not started");
            queries.start();
            server.start();
            queries.workerManager().registerWorker(TestWorkerInfoBuilder.active(
                    "control-worker", "127.0.0.1", server.getURI().getPort(), 0));
            assertThat(plane.schemaFingerprint(session(), COLUMNS)).hasSize(32);
            ownership.start("control-owner");
            assertThat(ownership.acquireQueryOwnership("control-query", "control-owner")).isTrue();
            var intent = intent(catalog, ownership.requireExecutionAnchor("control-query"));
            var context = context(intent);
            assertThatThrownBy(() -> plane.finalizeWrite(session(), new Output(), intent.fragments(), context))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("durable write intent");
            queries.writeCommitJournal().recordIntent(intent);
            assertThatThrownBy(() -> plane.finalizeWrite(session(), new Output(), intent.fragments(), context))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not started");
            ownershipRef.set(ownership);
            assertRejectedPublication(plane, queries, intent, responseOverride, status);
            assertPublication(plane, queries, ownership, intent, context, requestRef);
            bootstrap.close();
            assertThatThrownBy(() -> bootstrap.forCatalog(catalog)).hasMessageContaining("closed");
            assertThatThrownBy(() -> plane.schemaFingerprint(session(), COLUMNS)).hasMessageContaining("closed");
        } finally {
            server.stop();
            queries.shutdown();
        }
    }

    private static void assertPublication(NativeDatasetControlPlane plane, QueryBootstrap queries,
            HighAvailabilityCoordinationService ownership, WriteCommitEntry intent,
            DistributedWriteCommitContext context, AtomicReference<FinalizeDistributedWriteRequest> requestRef) {
        var publication = plane.finalizeWrite(session(), new Output(), intent.fragments(), context);
        assertThat(publication.outcome()).isEqualTo(NativeWriteFinalization.Outcome.COMMITTED);
        assertThat(queries.writeCommitJournal().getEntry(intent.queryId()).orElseThrow().publication())
                .isEqualTo(publication);
        assertThat(requestRef.get().getControlEpoch()).isEqualTo(ownership.getCurrentEpoch());
        assertThat(requestRef.get().getExecutionOwner().getFencingToken()).isEqualTo(intent.executionFencingToken());
        assertThat(requestRef.get().getDeadlineEpochMs()).isLessThanOrEqualTo(
                ownership.requireExecutionAnchor(intent.queryId()).expiresAt().toEpochMilli());
    }

    private static void assertRejectedPublication(NativeDatasetControlPlane plane, QueryBootstrap queries,
            WriteCommitEntry intent, AtomicReference<RuntimeCommandResponse> responseOverride, AtomicInteger status) {
        var context = context(intent);
        responseOverride.set(RuntimeCommandResponse.newBuilder().setError(ExecutionError.newBuilder()
                .setErrorClass(ErrorClass.ERROR_CLASS_INTERNAL).setMessage("publication interrupted")).build());
        status.set(HttpURLConnection.HTTP_UNAUTHORIZED);
        assertThatThrownBy(() -> plane.finalizeWrite(session(), new Output(), intent.fragments(), context))
                .isInstanceOf(SecurityException.class);
        assertThat(queries.writeCommitJournal().getEntry(intent.queryId()).orElseThrow().publication()).isNull();
        status.set(HttpURLConnection.HTTP_OK);
        var uncertain = plane.finalizeWrite(session(), new Output(), intent.fragments(), context);
        assertThat(uncertain).isEqualTo(
                new NativeWriteFinalization(NativeWriteFinalization.Outcome.UNKNOWN, 0, new byte[0]));
        responseOverride.set(RuntimeCommandResponse.newBuilder().setSuccess(true)
                .setPayload(Any.pack(DatasetSchemaFingerprintResult.getDefaultInstance())).build());
        assertThat(plane.finalizeWrite(session(), new Output(), intent.fragments(), context)).isEqualTo(uncertain);
        assertThat(queries.writeCommitJournal().getEntry(intent.queryId()).orElseThrow().publication()).isNull();
        responseOverride.set(null);
    }

    private static Server server(AtomicReference<FinalizeDistributedWriteRequest> requestRef,
            AtomicReference<RuntimeCommandResponse> responseOverride, AtomicInteger status) {
        var server = new Server(new InetSocketAddress("127.0.0.1", 0));
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse result)
                    throws IOException {
                assertThat(request.getHeader("X-Internal-Auth")).isEqualTo("credential");
                var command = RuntimeCommand.parseFrom(request.getInputStream().readAllBytes());
                assertThat(command.getProtocol()).isEqualTo(CanonicalTaskRequests.protocolVersion());
                var override = responseOverride.get();
                byte[] response = override != null ? override.toByteArray() : RuntimeCommandResponse.newBuilder()
                        .setSuccess(true).setPayload(responsePayload(command, requestRef)).build().toByteArray();
                result.setStatus(status.get());
                result.getOutputStream().write(response);
                base.setHandled(true);
            }
        });
        return server;
    }

    private static Any responsePayload(RuntimeCommand command,
            AtomicReference<FinalizeDistributedWriteRequest> requestRef) throws IOException {
        if (command.getPayload().is(FinalizeDistributedWriteRequest.class)) {
            var request = command.getPayload().unpack(FinalizeDistributedWriteRequest.class);
            requestRef.set(request);
            return Any.pack(FinalizeDistributedWriteResult.newBuilder()
                    .setCommitFingerprint(request.getCommitFingerprint()).setPublishedVersion(2)
                    .setManifestSha256(CanonicalTaskRequests.sha256(new byte[] {1}))
                    .setOutcome(WritePublicationOutcome.WRITE_PUBLICATION_OUTCOME_COMMITTED).build());
        }
        return Any.pack(DatasetSchemaFingerprintResult.newBuilder()
                .setPhysicalSchemaSha256(CanonicalTaskRequests.sha256(new byte[] {1})).build());
    }

    private static WriteCommitEntry intent(CatalogDescriptor catalog, ExecutionAnchor anchor) {
        var deployment = new CatalogDeploymentResponse(catalog.name(), catalog.provider(), catalog.incarnationId(),
                catalog.revision(), catalog.properties(), catalog.allowedTenants());
        var binding = LanceWriteBinding.newBuilder().setDatasetUri(allocation().datasetUri())
                .setOperation(LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND).setExpectedReadVersion(1)
                .setWriteId("write").setGeneration("generation").addWriteColumnNames("id")
                .setAllocationStagingPrefix("staging/write").setTransactionStagingPrefix("staging/write/attempt")
                .setSchemaFenceSha256(Checksum.newBuilder().setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(ByteString.copyFrom(new byte[32])))
                .setCatalog(CatalogExecutionBinding.newBuilder().setCatalogName(catalog.name())
                        .setConnectorProvider(catalog.provider()).setCatalogIncarnationId(catalog.incarnationId())
                        .setCatalogRevision(catalog.revision())
                        .setConfigurationFingerprint(CatalogDeploymentFingerprint.checksum(deployment))).build();
        byte[] bytes = WriteOutputBinding.newBuilder().setLance(binding).build().toByteArray();
        var fragment = LanceWriteFragment.newBuilder().setDatasetUri(allocation().datasetUri())
                .setTaskAttempt(org.pangrova.protocol.v2.TaskAttemptId.newBuilder()
                        .setQueryOrJobId(anchor.executionId())
                        .setExecutionOwner(ExecutionOwnerIdentity.newBuilder().setOwnerId(anchor.ownerId())
                                .setOwnerIncarnation(anchor.ownerIncarnation()).setFencingToken(anchor.fencingToken())))
                .build().toByteArray();
        var fragments = List.of(fragment);
        String fingerprint = WriteCommitFingerprint.compute(
                new org.pangrova.spi.coordination.WriteCommitFingerprint.Intent(
                        anchor.executionId(),
                        "tenant",
                        catalog.name(),
                        "schema.table",
                        "INSERT",
                        1,
                        bytes,
                        "alice",
                        org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt()),
                fragments);
        return new WriteCommitEntry(
                anchor.executionId(),
                catalog.name(),
                "schema.table",
                WriteCommitEntry.State.PENDING,
                System.currentTimeMillis(),
                fragments,
                "INSERT",
                fingerprint,
                "tenant",
                "alice",
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt(),
                1,
                bytes,
                fingerprint,
                anchor.ownerId(),
                anchor.ownerIncarnation(),
                anchor.fencingToken(),
                null);
    }

    private static DistributedWriteCommitContext context(WriteCommitEntry intent) {
        return new DistributedWriteCommitContext(
                intent.queryId(),
                intent.tenantId(),
                intent.baseSnapshotId(),
                intent.writeOutputBinding(),
                intent.commitFingerprint(),
                "alice",
                org.pangrova.coordinator.testinfra.CoordinatorWriteReceiptFixtures.receipt());
    }

    private static CatalogDescriptor catalog() {
        return new CatalogDescriptor("catalog", CatalogType.CONNECTOR, "lance", CatalogState.ACTIVE,
                Map.of(), "", Set.of("tenant"));
    }

    private static ConnectorSession session() {
        return SessionContextConverter.toConnectorSession(new CoordinatorSessionContext("control-query",
                Map.of("user", "alice", "catalog", "catalog"), null, "tenant"));
    }

    private static NativeWriteAllocation allocation() {
        return new NativeWriteAllocation("file:///dataset/table.lance", NativeWriteAllocation.Operation.APPEND,
                1, new byte[32], "write", "staging/write", "generation", List.of("id"));
    }

    private record Output() implements ConnectorOutputTableHandle {

        @Override
        public Optional<SchemaTableName> targetTable() {
            return Optional.of(new SchemaTableName("schema", "table"));
        }

        @Override
        public String writeMode() {
            return "INSERT";
        }

        @Override
        public OptionalLong writeBaseSnapshotId() {
            return OptionalLong.of(1);
        }

        @Override
        public Optional<NativeWriteAllocation> nativeWriteAllocation() {
            return Optional.of(allocation());
        }
    }
}
