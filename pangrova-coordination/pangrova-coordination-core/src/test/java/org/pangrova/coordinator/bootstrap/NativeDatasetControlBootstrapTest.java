package org.pangrova.coordinator.bootstrap;

import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import com.google.protobuf.Any;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.lance.LanceManagedSourceTestExecution;
import org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.worker.TestWorkerInfoBuilder;
import org.pangrova.persistence.jdbc.coordination.JdbcDistributedLease;
import org.pangrova.persistence.jdbc.query.coordination.JdbcExecutionAnchorStore;
import org.pangrova.metadata.store.jdbc.catalog.JdbcMetadataStore;
import org.pangrova.observability.sdk.noop.NoOpTracingProvider;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintRequest;
import org.pangrova.protocol.v2.DatasetSchemaFingerprintResult;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.PrepareDistributedWriteRequest;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.pangrova.spi.connector.coordination.DistributedWriteCommitContext;
import org.pangrova.spi.connector.metadata.NativeDatasetControlPlane;
import org.pangrova.spi.connector.metadata.NativeWritePreparation;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.coordination.WriteCommitEntry;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.postgresql.ds.PGSimpleDataSource;

import java.net.HttpURLConnection;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PG ownership/source authority, official SDK preparation and authenticated binary HTTP lifecycle. */
class NativeDatasetControlBootstrapTest extends ContainerTestBase {
    @TempDir Path directory;
    private static final List<ConnectorTableMetadata.Column> COLUMNS =
            List.of(new ConnectorTableMetadata.Column("id", PangrovaType.INTEGER));

    @Test
    void lateBindingPublicationAndCloseFollowRealServiceLifecycle() throws Exception {
        var execution = LanceManagedSourceTestExecution.append(directory);
        var dataSource = dataSource(execution.databaseConnectionFile(directory.resolve("test-database.properties")));
        var queryRef = new AtomicReference<QueryBootstrap>();
        var ownershipRef = new AtomicReference<HighAvailabilityCoordinationService>();
        var requestRef = new AtomicReference<PrepareDistributedWriteRequest>();
        var responseOverride = new AtomicReference<RuntimeCommandResponse>();
        var status = new AtomicInteger(HttpURLConnection.HTTP_OK);
        var catalog = new JdbcMetadataStore(dataSource).getCatalogDescriptor(execution.catalog().getCatalogName()).orElseThrow();
        var anchor = execution.originalAnchor();
        var session = session(execution);
        var server = server(execution, requestRef, responseOverride, status);
        var queries = new QueryBootstrap(CoordinatorBootstrapConfig.withDefaults(anchor.ownerId(), 0),
                new NoOpTracingProvider(), null, null, null, new ModuleIntegrationBootstrap(), dataSource,
                        ModelAuthorityFixtures.models());
        try (var processAuthority = new CoordinatorExecutionAuthority(new JdbcExecutionAnchorStore(dataSource),
                    anchor.ownerId(), anchor.ownerIncarnation(), Duration.ofSeconds(60));
             var ownership = new HighAvailabilityCoordinationService(new JdbcDistributedLease(dataSource),
                    new HighAvailabilityCoordinationService.CoordinationConfig(60_000, 20_000));
             var bootstrap = new NativeDatasetControlBootstrap("credential", queryRef::get, ownershipRef::get, dataSource,
                     ModelAuthorityFixtures.models())) {
            ownership.bindExecutionAuthority(processAuthority);
            NativeDatasetControlPlane plane = bootstrap.forCatalog(catalog).orElseThrow();
            assertThatThrownBy(() -> plane.schemaFingerprint(session, COLUMNS))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not started");
            queryRef.set(queries);
            assertThatThrownBy(() -> plane.schemaFingerprint(session, COLUMNS))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not started");
            queries.start(); server.start();
            queries.workerManager().registerWorker(TestWorkerInfoBuilder.active(
                    "control-worker", "127.0.0.1", server.getURI().getPort(), 0));
            assertThat(plane.schemaFingerprint(session, COLUMNS)).isEqualTo(
                    execution.schemaFingerprint().getPhysicalSchemaSha256().getDigest().toByteArray()).hasSize(32);
            ownership.start(anchor.ownerId());
            assertThat(ownership.acquireQueryOwnership(anchor.executionId(), anchor.ownerId())).isTrue();
            var intent = execution.intent(); var context = intent.commitContext();
            var missing = new DistributedWriteCommitContext(context.executionId() + "-missing", context.tenantId(),
                    context.baseSnapshotId(), context.writeOutputBinding(), context.commitFingerprint(),
                    context.principal(), context.authorizationReceipt(), null);
            assertThatThrownBy(() -> plane.prepareWrite(session, execution.output(), intent.fragments(), missing, execution.reservation()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("durable write intent");
            assertThat(queries.writeCommitJournal().getEntry(intent.queryId()).orElseThrow().commitFingerprint())
                    .isEqualTo(intent.commitFingerprint());
            assertThatThrownBy(() -> plane.prepareWrite(session, execution.output(), intent.fragments(), context, execution.reservation()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not started");
            ownershipRef.set(ownership);
            assertRejectedPreparation(plane, session, execution, responseOverride, status);
            var prepared = plane.prepareWrite(session, execution.output(), intent.fragments(), context, execution.reservation());
            assertThat(prepared.outcome()).isEqualTo(NativeWritePreparation.Outcome.PREPARED);
            assertThat(queries.writeCommitJournal().getEntry(intent.queryId()).orElseThrow().publication()).isNull();
            assertThat(requestRef.get().getControlEpoch()).isEqualTo(ownership.getCurrentEpoch());
            assertThat(requestRef.get().getExecutionOwner().getFencingToken()).isEqualTo(intent.executionFencingToken());
            assertThat(requestRef.get().getDeadlineEpochMs()).isLessThanOrEqualTo(
                    ownership.requireExecutionAnchor(intent.queryId()).expiresAt().toEpochMilli());
            var accepted = execution.accept();
            var committed = queries.writeCommitJournal().getEntry(intent.queryId()).orElseThrow();
            assertThat(committed.state()).isEqualTo(WriteCommitEntry.State.COMMITTED);
            assertThat(committed.publication().publishedVersion()).isEqualTo(accepted.getVersion()).isEqualTo(2);
            assertThat(committed.publication().manifestSha256()).isEqualTo(accepted.getManifestSha256().getDigest().toByteArray());
            assertThat(prepared.publication().orElseThrow().candidate().manifestSha256()).isEqualTo(committed.publication().manifestSha256());
            plane.cleanupSourceObjects(session, execution.reservation());
            try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                    "SELECT count(*) FROM managed_source_prepare_invocations WHERE fingerprint=? AND state='ISSUED'")) {
                statement.setString(1, intent.commitFingerprint());
                try (var result = statement.executeQuery()) { result.next(); assertThat(result.getLong(1)).isGreaterThan(0); }
            }
            assertThat(Files.isRegularFile(Path.of(accepted.getDatasetUri()).resolve(accepted.getManifestRelativePath()))).isTrue();
            bootstrap.close();
            assertThatThrownBy(() -> bootstrap.forCatalog(catalog)).hasMessageContaining("closed");
            assertThatThrownBy(() -> plane.schemaFingerprint(session, COLUMNS)).hasMessageContaining("closed");
        } finally { server.stop(); queries.shutdown(); }
    }

    @Test
    void successfulBinarySdkJoinReceiptsReachRealPgAndCleanupDeletesOnlyWinnerSidecars() throws Exception {
        var execution = LanceManagedSourceTestExecution.create(directory);
        var dataSource = dataSource(execution.databaseConnectionFile(directory.resolve("cleanup-database.properties")));
        var queryRef = new AtomicReference<QueryBootstrap>();
        var ownershipRef = new AtomicReference<HighAvailabilityCoordinationService>();
        var requestRef = new AtomicReference<PrepareDistributedWriteRequest>();
        var server = server(execution, requestRef, new AtomicReference<>(), new AtomicInteger(HttpURLConnection.HTTP_OK));
        var anchor = execution.originalAnchor();
        var queries = new QueryBootstrap(CoordinatorBootstrapConfig.withDefaults(anchor.ownerId(), 0),
                new NoOpTracingProvider(), null, null, null, new ModuleIntegrationBootstrap(), dataSource,
                        ModelAuthorityFixtures.models());
        try (var processAuthority = new CoordinatorExecutionAuthority(new JdbcExecutionAnchorStore(dataSource),
                    anchor.ownerId(), anchor.ownerIncarnation(), Duration.ofSeconds(60));
             var ownership = new HighAvailabilityCoordinationService(new JdbcDistributedLease(dataSource),
                    new HighAvailabilityCoordinationService.CoordinationConfig(60_000, 20_000));
             var bootstrap = new NativeDatasetControlBootstrap("credential", queryRef::get, ownershipRef::get, dataSource,
                     ModelAuthorityFixtures.models())) {
            ownership.bindExecutionAuthority(processAuthority); queries.start(); queryRef.set(queries);
            ownership.start(anchor.ownerId()); ownershipRef.set(ownership);
            assertThat(ownership.acquireQueryOwnership(anchor.executionId(), anchor.ownerId())).isTrue();
            server.start(); queries.workerManager().registerWorker(TestWorkerInfoBuilder.active(
                    "cleanup-worker", "127.0.0.1", server.getURI().getPort(), 0));
            var descriptor = new JdbcMetadataStore(dataSource).getCatalogDescriptor(execution.catalog().getCatalogName()).orElseThrow();
            var plane = bootstrap.forCatalog(descriptor).orElseThrow(); var intent = execution.intent(); var session = session(execution);
            assertThat(plane.prepareWrite(session, execution.output(), intent.fragments(), intent.commitContext(), execution.reservation()).outcome())
                    .isEqualTo(NativeWritePreparation.Outcome.PREPARED);
            assertThat(requestRef.get().getInvocationId()).matches("[0-9a-f]{32}");
            var accepted = execution.accept();
            plane.cleanupSourceObjects(session, execution.reservation());
            plane.cleanupSourceObjects(session, execution.reservation());
            try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                    "SELECT state,count(*) FROM managed_source_prepare_invocations WHERE fingerprint=? GROUP BY state")) {
                statement.setString(1, intent.commitFingerprint());
                try (var result = statement.executeQuery()) {
                    assertThat(result.next()).isTrue(); assertThat(result.getString(1)).isEqualTo("DRAINED");
                    assertThat(result.getLong(2)).isEqualTo(2); assertThat(result.next()).isFalse();
                }
            }
            int sidecars = 0;
            for (var page : execution.pages()) for (var object : page.getObjectPage().getObjectsList()) {
                var path = Path.of(accepted.getDatasetUri()).resolve(object.getRelativePath());
                if (object.getPurpose() == org.pangrova.protocol.v2.SourceObjectPurpose.SOURCE_OBJECT_PURPOSE_WINNER_SIDECAR) {
                    sidecars++; assertThat(Files.exists(path)).isFalse();
                } else assertThat(Files.size(path)).isEqualTo(object.getSizeBytes());
            }
            assertThat(sidecars).isEqualTo(1);
            assertThat(execution.intent().state()).isEqualTo(WriteCommitEntry.State.COMMITTED);
            assertThat(execution.intent().publication().manifestSha256()).isEqualTo(accepted.getManifestSha256().getDigest().toByteArray());
        } finally { server.stop(); queries.shutdown(); }
    }

    private static void assertRejectedPreparation(NativeDatasetControlPlane plane, ConnectorSession session,
            LanceManagedSourceTestExecution execution, AtomicReference<RuntimeCommandResponse> responseOverride,
            AtomicInteger status) {
        var intent = execution.intent(); var context = intent.commitContext();
        responseOverride.set(RuntimeCommandResponse.newBuilder().setError(ExecutionError.newBuilder()
                .setErrorClass(ErrorClass.ERROR_CLASS_INTERNAL).setMessage("preparation interrupted")).build());
        status.set(HttpURLConnection.HTTP_UNAUTHORIZED);
        assertThatThrownBy(() -> plane.prepareWrite(session, execution.output(), intent.fragments(), context, execution.reservation()))
                .isInstanceOf(SecurityException.class);
        assertThat(execution.intent().publication()).isNull();
        status.set(HttpURLConnection.HTTP_OK);
        var uncertain = plane.prepareWrite(session, execution.output(), intent.fragments(), context, execution.reservation());
        assertThat(uncertain).isEqualTo(new NativeWritePreparation(NativeWritePreparation.Outcome.UNKNOWN, java.util.Optional.empty()));
        responseOverride.set(RuntimeCommandResponse.newBuilder().setSuccess(true)
                .setPayload(Any.pack(DatasetSchemaFingerprintResult.getDefaultInstance())).build());
        assertThat(plane.prepareWrite(session, execution.output(), intent.fragments(), context, execution.reservation())).isEqualTo(uncertain);
        assertThat(execution.intent().publication()).isNull();
        responseOverride.set(null);
    }

    private static Server server(LanceManagedSourceTestExecution execution, AtomicReference<PrepareDistributedWriteRequest> requestRef,
            AtomicReference<RuntimeCommandResponse> responseOverride, AtomicInteger status) {
        var server = new Server(new InetSocketAddress("127.0.0.1", 0));
        server.setHandler(new AbstractHandler() {
            @Override public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse result)
                    throws IOException {
                assertThat(request.getHeader("X-Internal-Auth")).isEqualTo("credential");
                var command = RuntimeCommand.parseFrom(request.getInputStream().readAllBytes());
                assertThat(command.getProtocol()).isEqualTo(CanonicalProtocolIdentity.current());
                var override = responseOverride.get();
                try {
                    byte[] response = override != null ? override.toByteArray() : RuntimeCommandResponse.newBuilder()
                            .setSuccess(true).setPayload(responsePayload(command, execution, requestRef)).build().toByteArray();
                    result.setStatus(status.get()); result.getOutputStream().write(response); base.setHandled(true);
                } catch (Exception failure) { throw new IOException("Actual SDK HTTP fixture failed", failure); }
            }
        });
        return server;
    }

    private static Any responsePayload(RuntimeCommand command, LanceManagedSourceTestExecution execution,
            AtomicReference<PrepareDistributedWriteRequest> requestRef) throws Exception {
        if (command.getPayload().is(PrepareDistributedWriteRequest.class)) {
            var request = command.getPayload().unpack(PrepareDistributedWriteRequest.class);
            requestRef.set(request);
            return Any.pack(execution.preparePage(request));
        }
        if (command.getPayload().is(org.pangrova.protocol.v2.CleanupSourceObjectsRequest.class)) {
            return Any.pack(execution.cleanupPage(command.getPayload().unpack(org.pangrova.protocol.v2.CleanupSourceObjectsRequest.class)));
        }
        var request = command.getPayload().unpack(DatasetSchemaFingerprintRequest.class);
        var actualSchema = org.pangrova.planner.substrait.NativeDatasetSchemaCompiler.compile(COLUMNS);
        assertThat(request.getSchema()).isEqualTo(actualSchema.schema());
        assertThat(request.getColumnTypesList()).isEqualTo(actualSchema.columnTypes());
        assertThat(request.getCatalog()).isEqualTo(execution.catalog());
        return Any.pack(execution.schemaFingerprint());
    }

    private static PGSimpleDataSource dataSource(Path file) throws IOException {
        var properties = new Properties();
        try (var input = Files.newInputStream(file)) { properties.load(input); }
        var dataSource = new PGSimpleDataSource();
        dataSource.setURL(properties.getProperty("url")); dataSource.setUser(properties.getProperty("user"));
        dataSource.setPassword(properties.getProperty("password")); return dataSource;
    }
    private static ConnectorSession session(LanceManagedSourceTestExecution execution) {
        var intent = execution.intent();
        return SessionContextConverter.toConnectorSession(new CoordinatorSessionContext(intent.queryId(),
                Map.of("user", intent.principal(), "catalog", intent.catalogName()), null, intent.tenantId()));
    }
}
