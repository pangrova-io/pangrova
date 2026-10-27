package org.pangrova.testing.e2e.lifecycle;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.ListAppender;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.http.EmbeddedHttpServerFactory;
import org.pangrova.common.http.PemMutualTls;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.coordination.CoordinatorExecutionAuthority;
import org.pangrova.coordinator.coordination.HighAvailabilityCoordinationService;
import org.pangrova.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.coordinator.http.endpoint.CoordinatorExceptionMapper;
import org.pangrova.coordinator.http.endpoint.copy.CopySourceReadResource;
import org.pangrova.coordinator.http.endpoint.copy.CopySourceWireLoggingPolicy;
import org.pangrova.metadata.coordination.JdbcExecutionAnchorStore;
import org.pangrova.metadata.coordination.JdbcDistributedLease;
import org.pangrova.metadata.storage.JdbcCopySourceReadAdmissions;
import org.pangrova.metadata.storage.JdbcStorageProfileRegistry;
import org.pangrova.metadata.store.SchemaMigrationRunner;
import org.pangrova.metadata.tenant.JdbcTenantStore;
import org.pangrova.protocol.rest.copy.CopySourceReadHttp;
import org.pangrova.protocol.rest.copy.PrepareCopySourceRequest;
import org.pangrova.protocol.rest.copy.PrepareCopySourceResponse;
import org.pangrova.protocol.v2.SourcePreparationRequest;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.Checksum;
import org.pangrova.protocol.v2.ExecutionOwnerIdentity;
import org.pangrova.protocol.v2.SecurityContext;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.security.authn.AuthenticationManager;
import org.pangrova.security.authn.OAuth2Authenticator;
import org.pangrova.security.authz.DynamicRbacAccessControl;
import org.pangrova.security.authz.Privilege;
import org.pangrova.security.authz.PrivilegeGrant;
import org.pangrova.security.authz.Role;
import org.pangrova.security.authz.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.store.PersistentColumnMaskStore;
import org.pangrova.security.store.PersistentGrantStore;
import org.pangrova.security.store.PersistentRoleStore;
import org.pangrova.security.store.PersistentRowPolicyStore;
import org.pangrova.security.tenant.model.ResourceQuota;
import org.pangrova.security.tenant.model.Tenant;
import org.pangrova.security.tenant.model.TenantRole;
import org.pangrova.security.tenant.model.TenantStatus;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageType;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.worker.ai.blob.S3BlobStorage;
import org.pangrova.worker.copy.CopySourceReadClient;
import org.pangrova.worker.copy.CopySourceStagingCommand;
import org.pangrova.worker.http.WorkerHttpServer;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real PostgreSQL, authenticated HTTP, Worker command and versioned MinIO staging. */
class CopySourceReadHttpE2ETest extends ContainerTestBase {
    private static final String ACCESS_KEY = "copy-http-e2e";
    private static final String SECRET_KEY = "copy-http-e2e-secret";
    private static final org.pangrova.shuffle.exchange.ExchangeTransport NO_EXCHANGE =
            new org.pangrova.shuffle.exchange.ExchangeTransport() {
                @Override public void bindInputs(long executorHandle,
                        org.pangrova.shuffle.exchange.ExchangeTaskAttempt attempt,
                        List<org.pangrova.shuffle.exchange.ExchangeInput> inputs) {
                    throw new AssertionError("COPY staging must not bind Native exchange");
                }
                @Override public void releaseInputs(long executorHandle) {}
            };

    @TempDir Path stagingRoot;
    private Level priorJettyLevel;

    @BeforeEach
    void protectTicketWireLogging() {
        Logger jetty = (Logger) LoggerFactory.getLogger("org.eclipse.jetty");
        priorJettyLevel = jetty.getLevel();
        jetty.setLevel(Level.WARN);
        CopySourceWireLoggingPolicy.requireSafeJettyLogging();
    }

    @AfterEach
    void restoreTicketWireLogging() {
        ((Logger) LoggerFactory.getLogger("org.eclipse.jetty")).setLevel(priorJettyLevel);
    }

    @Test
    @org.junit.jupiter.api.Tag("native-required")
    @org.junit.jupiter.api.Tag("native-runtime-owner")
    @org.junit.jupiter.api.parallel.ResourceLock("pangrova-packaged-native-runtime")
    void privateTicketStagesExactVersionAndRestartsFromManagedBytes() throws Exception {
        try (var minio = new GenericContainer<>("minio/minio:RELEASE.2024-12-18T13-15-44Z")
                    .withExposedPorts(9000)
                    .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                    .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                    .withEnv("MINIO_KMS_SECRET_KEY",
                            "copy-http-e2e-key:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
                    .withCommand("server", "/data")) {
            minio.start();
            URI endpoint = minioEndpoint(minio);
            try (var authority = new BootstrapSecurityAuthority();
                    var fixture = new Fixture(authority)) {
                String sourceBucket = "copy-source-" + UUID.randomUUID();
                String managedBucket = "copy-managed-" + UUID.randomUUID();
                String sourceUrl = "s3://" + sourceBucket + "/data/part.parquet";
                byte[] bytes = actualParquetBytes();
                try (S3Client objects = objects(endpoint)) {
                    objects.createBucket(request -> request.bucket(sourceBucket));
                    objects.putBucketVersioning(request -> request.bucket(sourceBucket)
                            .versioningConfiguration(VersioningConfiguration.builder()
                                    .status(BucketVersioningStatus.ENABLED).build()));
                    objects.createBucket(request -> request.bucket(managedBucket));
                    String version = objects.putObject(request -> request.bucket(sourceBucket)
                            .key("data/part.parquet"), RequestBody.fromBytes(bytes)).versionId();
                    assertThat(version).isNotBlank().isNotEqualTo("null");
                    fixture.createProfile(sourceBucket, endpoint);
                    var revision = fixture.profiles.getCurrentRevision("incoming").orElseThrow();
                    try (var managed = new S3BlobStorage(objects, new S3BlobStorage.Settings(
                            managedBucket, "us-east-1", null, "copy-managed",
                            new S3BlobStorage.Encryption(ServerSideEncryption.AES256, ""), "", true))) {
                        var scope = fixture.scope(revision.incarnationId(), revision.revision(),
                                sourceUrl, managed.rootUri(), managed.backendIdentitySha256());
                        var ticket = fixture.admissions.issue(scope, fixture.identity, fixture.anchor);
                        var wireScope = fixture.wire(scope);
                        assertThat(wireScope.commandSha256())
                                .isEqualTo(JdbcCopySourceReadAdmissions.commandSha256(scope));
                        var client = CopySourceReadClient.forLoopbackTest(
                                fixture.baseUri(), authority.secret());
                        var command = CopySourceStagingCommand.forLoopbackTest(
                                client, credentials(), managed, stagingRoot, 1024 * 1024);
                        // The same logical s3:// URI on another physical endpoint
                        // must fail before redeem or any managed-object I/O.
                        try (var otherBackend = new S3BlobStorage(
                                new S3BlobStorage.Settings(managedBucket, "us-east-1",
                                        URI.create("https://another-store.example"), "copy-managed",
                                        new S3BlobStorage.Encryption(ServerSideEncryption.AES256, ""),
                                        "", true))) {
                            assertThat(otherBackend.rootUri()).isEqualTo(managed.rootUri());
                            assertThat(otherBackend.backendIdentitySha256())
                                    .isNotEqualTo(managed.backendIdentitySha256());
                            var redirected = CopySourceStagingCommand.forLoopbackTest(
                                    client, credentials(), otherBackend, stagingRoot, 1024 * 1024);
                            assertThatThrownBy(() -> redirected.execute(ticket.nonce(), wireScope, preparationBudget(wireScope)))
                                    .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class)
                                    .hasMessageContaining("admitted backend");
                        }

                        assertThatThrownBy(() -> CopySourceReadClient.forLoopbackTest(
                                fixture.baseUri(), "wrong-secret")
                                .redeem(ticket.nonce(), wireScope))
                                .isInstanceOf(IllegalStateException.class).hasMessageContaining("status 401");
                        assertThatThrownBy(() -> client.redeem(ticket.nonce(),
                                fixture.wire(new JdbcCopySourceReadAdmissions.Scope(scope.tenantId(),
                                        scope.principal(), scope.profileName(), scope.profileIncarnationId(),
                                        scope.profileRevision(), scope.canonicalSourceUrl(),
                                        "s3://untrusted/copy-managed",
                                        scope.managedBackendIdentitySha256(), scope.executionId(),
                                        scope.stagingAttemptId(), scope.ownerId(), scope.ownerIncarnation(),
                                        scope.controlEpoch(), scope.ownerFencingToken(),
                                        scope.deadlineEpochMs()))))
                                .isInstanceOf(IllegalStateException.class).hasMessageContaining("status 403");

                        assertThatThrownBy(() -> {
                            try (var premature = command.executeForNative(ticket.nonce(), wireScope)) {
                                assertThat(premature.path()).exists();
                            }
                        }).isInstanceOf(IllegalStateException.class)
                                .hasMessageContaining("previously staged source proof");
                        assertThat(client.redeem(ticket.nonce(), wireScope).stagedSource()).isNull();

                        var certs = certificates();
                        CopySourceReadHttp.StagedSource proof;
                        try (var nativeOwner = new CopyNativeInputAssertions.Runtime(command)) {
                            var tasks = nativeOwner.manager;
                            var stagedTask = copyTask(wireScope, ticket.nonce(),
                                    tasks.workerExecutionIdentity().toProto());
                            tasks.requireControlEpochHandshake();
                            tasks.observeControlEpoch(wireScope.controlEpoch());
                            var worker = new WorkerHttpServer(
                                    new WorkerHttpServer.WorkerHttpConfig(0, "localhost", 0,
                                            authority.secret(), "grpc",
                                            new WorkerHttpServer.CopyTaskTls(
                                                    certs.serverCert(), certs.serverKey(), certs.ca())),
                                    tasks, NO_EXCHANGE, null, null, null,
                                    nativeOwner.inputs);
                            try {
                                worker.start();
                                var stageClient = HttpClient.newBuilder().sslContext(
                                        PemMutualTls.context(certs.clientCert(),
                                                certs.clientKey(), certs.ca())).build();
                                var stageUri = URI.create("https://localhost:"
                                        + worker.getCopyTaskTlsPort() + "/v1/task/copy-stage");
                                proof = postStage(stageClient, stageUri, authority.secret(), stagedTask);
                                var prepared = postPreparation(stageClient, stageUri, authority.secret(), stagedTask);
                                assertThat(prepared.getUsage().getObjectStoreReadBytes()).isEqualTo(2L * bytes.length);
                                assertThat(prepared.getUsage().getObjectStoreWriteBytes()).isEqualTo(bytes.length);
                                assertThat(prepared.getUsage().getSpillBytes()).isEqualTo(bytes.length);
                                assertThat(postStage(stageClient, stageUri, authority.secret(), stagedTask))
                                        .isEqualTo(proof);
                                objects.deleteObject(request -> request.bucket(sourceBucket)
                                        .key("data/part.parquet").versionId(version));
                                assertThat(postStage(stageClient, stageUri, authority.secret(), stagedTask))
                                        .isEqualTo(proof);
                                assertThat(nativeOwner.nativeRuntime.nodePool().getUsedBytes()).isZero();
                                assertThat(nativeOwner.quota.getTenantActiveQueryCount(wireScope.tenantId())).isZero();
                                assertThat(proof.sourceVersionId()).isEqualTo(version);
                                assertThat(proof.managedObjectUri()).startsWith(managed.rootUri() + "/tenants/");
                                try (var input = managed.open(proof.tenantScope(), proof.contentSha256(),
                                        proof.contentSize())) {
                                    assertThat(input.readAllBytes()).isEqualTo(bytes);
                                }
                                assertThat(command.execute(ticket.nonce(), wireScope, preparationBudget(wireScope))).isEqualTo(proof);
                                java.nio.file.Path localPath;
                                try (var local = command.executeForNative(ticket.nonce(), wireScope)) {
                                    localPath = local.path();
                                    assertThat(local.proof()).isEqualTo(proof);
                                    assertThat(java.nio.file.Files.readAllBytes(localPath)).isEqualTo(bytes);
                                }
                                assertThat(localPath).doesNotExist();
                                assertThat(client.redeem(ticket.nonce(), wireScope).stagedSource()).isEqualTo(proof);

                                var frozenProof = proof;
                                try (var dispatcher = new org.pangrova.coordinator.scheduler.RemoteTaskDispatcher(
                                        org.pangrova.common.resilience.CircuitBreakerPolicy.noOp(),
                                        new org.pangrova.observability.noop.NoOpTracingProvider(), authority.secret())) {
                                    dispatcher.setCopySourceBinder(new org.pangrova.coordinator.query.execution.dispatch.dag.binding.CopySourceTaskBinder(
                                            fixture.admissions, execution -> fixture.anchor));
                                    var wrongSource = CopyNativeInputAssertions.pendingTask(wireScope, frozenProof, 0)
                                            .taskRequest().toBuilder();
                                    wrongSource.getSplitsBuilder(0).setSourceSnapshot("not-the-frozen-version");
                                    assertThatThrownBy(() -> dispatcher.bindSourceInput(SubmitTaskRequest.from(wrongSource.build())))
                                            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("frozen source proof");
                                    CopyNativeInputAssertions.verify(nativeOwner, grant -> {
                                        var pending = SubmitTaskRequest.from(nativeOwner.nativeRuntime.bindWorker(
                                                CopyNativeInputAssertions.pendingTask(wireScope, frozenProof, grant)
                                                        .taskRequest()));
                                        var bound = dispatcher.bindSourceInput(dispatcher.bindControlIdentity(pending));
                                        assertThat(dispatcher.bindSourceInput(bound)).isSameAs(bound);
                                        assertThat(bound.copySourceRead()).isNotNull();
                                        var redelivery = dispatcher.bindSourceInput(pending);
                                        assertThat(redelivery.canonicalBytes()).isEqualTo(bound.canonicalBytes());
                                        assertThat(redelivery.copySourceRead().nonce()).isNotEqualTo(bound.copySourceRead().nonce());
                                        return bound;
                                    }, stagingRoot, proof.contentSize());
                                }

                                fixture.grants.revokeGrants("reader", Privilege.READ_STORAGE_PROFILE,
                                        "incoming", null, null);
                                var deniedPreparation = postPreparation(stageClient, stageUri, authority.secret(), stagedTask);
                                assertThat(deniedPreparation.getError().getErrorClass())
                                        .isEqualTo(org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_POLICY_DENIED);
                                assertThat(deniedPreparation.getUsage()).isEqualTo(prepared.getUsage());
                                assertThatThrownBy(() -> command.execute(ticket.nonce(), wireScope, preparationBudget(wireScope)))
                                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("status 403");
                                assertThatThrownBy(() -> command.executeForNative(ticket.nonce(), wireScope))
                                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("status 403");
                                fixture.grants.addGrant(PrivilegeGrant.of("reader", Privilege.READ_STORAGE_PROFILE,
                                        "STORAGE_PROFILE", "incoming", null, null, null));
                                assertThatThrownBy(() -> command.execute(ticket.nonce(), wireScope, preparationBudget(wireScope)))
                                        .isInstanceOf(IllegalStateException.class).hasMessageContaining("status 403");
                            } finally {
                                worker.stop();
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    void credentialBearingEndpointIsRejectedBeforeTicketIssue() throws Exception {
        try (var authority = new BootstrapSecurityAuthority();
                var fixture = new Fixture(authority)) {
            String secret = "do-not-disclose";
            fixture.createProfile("incoming-secret", "copy-source-secret",
                    "https://reader:" + secret + "@example.invalid");
            var revision = fixture.profiles.getCurrentRevision("incoming-secret").orElseThrow();
            var scope = fixture.scope("incoming-secret", revision.incarnationId(), revision.revision(),
                    "s3://copy-source-secret/data/part.parquet", "s3://managed/copy-managed");
            assertThatThrownBy(() -> fixture.admissions.issue(
                    scope, fixture.identity, fixture.anchor))
                    .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class)
                    .hasMessageNotContaining(secret);
        }
    }

    @Test
    void malformedPrivateRedeemBodyNeverEchoesBearerNonce() throws Exception {
        try (var authority = new BootstrapSecurityAuthority();
                var fixture = new Fixture(authority)) {
            String nonce = "private-nonce-" + UUID.randomUUID();
            Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
            Level priorRootLevel = root.getLevel();
            Logger jetty = (Logger) LoggerFactory.getLogger("org.eclipse.jetty");
            var captured = new ListAppender<ILoggingEvent>();
            captured.start();
            root.addAppender(captured);
            root.setLevel(Level.DEBUG);
            // Simulate an operator changing the logger after the endpoint starts.
            // The installed TurboFilter must still suppress Jetty's raw buffers.
            jetty.setLevel(Level.DEBUG);
            try {
                root.debug("COPY ticket log capture active");
                assertThat(captured.list).anySatisfy(event ->
                        assertThat(event.getFormattedMessage())
                                .contains("COPY ticket log capture active"));
                var request = HttpRequest.newBuilder(URI.create(fixture.baseUri()
                                + "/v1/internal/copy-source-read/redeem"))
                        .header("Authorization", "Bearer " + authority.secret())
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"nonce\":\"" + nonce + "\",\"scope\":"))
                        .build();
                var response = HttpClient.newHttpClient().send(request,
                        HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isGreaterThanOrEqualTo(400);
                assertThat(response.body()).doesNotContain(nonce);
                assertThat(captured.list).allSatisfy(event -> {
                    assertThat(event.getFormattedMessage()).doesNotContain(nonce);
                    if (event.getThrowableProxy() != null) {
                        assertThat(ThrowableProxyUtil.asString(event.getThrowableProxy()))
                                .doesNotContain(nonce);
                    }
                });
            } finally {
                jetty.setLevel(Level.WARN);
                root.setLevel(priorRootLevel);
                root.detachAppender(captured);
                captured.stop();
            }
        }
    }

    @Test
    void jettyWireDebugRejectsCopyEndpointBeforeListening() {
        Logger jetty = (Logger) LoggerFactory.getLogger("org.eclipse.jetty");
        Logger parser = (Logger) LoggerFactory.getLogger("org.eclipse.jetty.http.HttpParser");
        Level priorParserLevel = parser.getLevel();
        jetty.setLevel(Level.DEBUG);
        try {
            assertThatThrownBy(CopySourceWireLoggingPolicy::requireSafeJettyLogging)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Jetty wire logging");
            jetty.setLevel(Level.WARN);
            parser.setLevel(Level.DEBUG);
            assertThatThrownBy(CopySourceWireLoggingPolicy::requireSafeJettyLogging)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Jetty wire logging");
        } finally {
            parser.setLevel(priorParserLevel);
            jetty.setLevel(Level.WARN);
        }
    }

    private static StaticCredentialsProvider credentials() {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY));
    }

    private static PrepareCopySourceRequest copyTask(CopySourceReadHttp.Scope scope, String nonce,
            org.pangrova.protocol.v2.WorkerExecutionIdentity workerIdentity) {
        var canonical = SourcePreparationRequest.newBuilder()
                .setProtocol(CanonicalTaskRequests.protocolVersion())
                .setAttempt(TaskAttemptId.newBuilder()
                        .setQueryOrJobId(scope.executionId())
                        .setTaskId(scope.stagingAttemptId())
                        .setPlanEpoch(1)
                        .setFencingToken((1L << 32) | 1L)
                        .setControlEpoch(scope.controlEpoch())
                        .setWorkerExecutionIdentity(workerIdentity)
                        .setExecutionOwner(ExecutionOwnerIdentity.newBuilder()
                                .setOwnerId(scope.ownerId())
                                .setOwnerIncarnation(scope.ownerIncarnation())
                                .setFencingToken(scope.ownerFencingToken())))
                .setSecurity(SecurityContext.newBuilder()
                        .setTenantId(scope.tenantId()).setPrincipal(scope.principal()))
                .setSourceCommandSha256(Checksum.newBuilder()
                        .setAlgorithm(Checksum.Algorithm.ALGORITHM_SHA256)
                        .setDigest(com.google.protobuf.ByteString.copyFrom(
                                HexFormat.of().parseHex(scope.commandSha256()))))
                .setResourceGrant(preparationGrant(scope))
                .setDeadlineEpochMs(scope.deadlineEpochMs())
                .build();
        return PrepareCopySourceRequest.from(canonical,
                new CopySourceReadHttp.RedeemRequest(nonce, scope));
    }

    private static CopySourceReadHttp.StagedSource postStage(HttpClient client, URI uri,
            String secret, PrepareCopySourceRequest task) throws Exception {
        var result = postPreparation(client, uri, secret, task);
        assertThat(result.getIoJoined()).isTrue();
        assertThat(result.hasError()).withFailMessage(result.getError().toString()).isFalse();
        var source = result.getSource();
        return new CopySourceReadHttp.StagedSource(source.getSourceVersionId(), source.getManagedObjectUri(),
                source.getContentSha256(), source.getContentSize(), source.getTenantScope());
    }

    private static org.pangrova.protocol.v2.SourcePreparationResult postPreparation(HttpClient client, URI uri,
            String secret, PrepareCopySourceRequest task) throws Exception {
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .header("X-Internal-Auth", secret)
                .POST(HttpRequest.BodyPublishers.ofByteArray(
                        JsonMapperFactory.shared().writeValueAsBytes(task)))
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).withFailMessage(response.body()).isEqualTo(200);
        return JsonMapperFactory.shared().readValue(response.body(), PrepareCopySourceResponse.class).resultFor(task);
    }

    private static org.pangrova.protocol.v2.StageResourceGrant preparationGrant(CopySourceReadHttp.Scope scope) {
        return org.pangrova.protocol.v2.StageResourceGrant.newBuilder().setGrantId(scope.stagingAttemptId() + ":prepare")
                .setCpuSlots(1).setJvmHeapBytes(1024 * 1024).setObjectStoreReadBytes(2 * 1024 * 1024)
                .setObjectStoreWriteBytes(1024 * 1024).setSpillBytes(1024 * 1024).build();
    }

    private static org.pangrova.worker.copy.CopyPreparation preparationBudget(CopySourceReadHttp.Scope scope) {
        return new org.pangrova.worker.copy.CopyPreparation(preparationGrant(scope), scope.deadlineEpochMs(),
                java.time.Clock.systemUTC());
    }

    private CertificateFiles certificates() throws Exception {
        Path ca = stagingRoot.resolve("ca.pem");
        Path caKey = stagingRoot.resolve("ca.key");
        run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", caKey.toString(), "-out", ca.toString(),
                "-subj", "/CN=Pangrova test CA", "-days", "1");
        Path serverCert = issue("server", ca, caKey, "subjectAltName=DNS:localhost");
        Path clientCert = issue("client", ca, caKey, "extendedKeyUsage=clientAuth");
        return new CertificateFiles(ca, serverCert, stagingRoot.resolve("server.key"),
                clientCert, stagingRoot.resolve("client.key"));
    }

    private Path issue(String name, Path ca, Path caKey, String extension) throws Exception {
        Path key = stagingRoot.resolve(name + ".key");
        Path csr = stagingRoot.resolve(name + ".csr");
        Path cert = stagingRoot.resolve(name + ".pem");
        run("openssl", "genpkey", "-algorithm", "RSA", "-out", key.toString(),
                "-pkeyopt", "rsa_keygen_bits:2048");
        run("openssl", "req", "-new", "-key", key.toString(), "-out", csr.toString(),
                "-subj", "/CN=" + name);
        Path ext = stagingRoot.resolve(name + ".ext");
        java.nio.file.Files.writeString(ext, extension + System.lineSeparator());
        run("openssl", "x509", "-req", "-in", csr.toString(), "-CA", ca.toString(),
                "-CAkey", caKey.toString(), "-CAcreateserial", "-out", cert.toString(),
                "-days", "1", "-extfile", ext.toString());
        return cert;
    }

    private static void run(String... command) throws Exception {
        Process process = new ProcessBuilder(List.of(command)).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        if (process.waitFor() != 0) throw new IllegalStateException(output);
    }

    private record CertificateFiles(Path ca, Path serverCert, Path serverKey,
                                    Path clientCert, Path clientKey) {}

    private byte[] actualParquetBytes() throws Exception {
        Path file = stagingRoot.resolve("actual-source.parquet");
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
        DataWriter<Record> writer = Parquet.writeData(HadoopOutputFile.fromPath(
                        new org.apache.hadoop.fs.Path(file.toUri()), new Configuration()))
                .schema(schema).withSpec(PartitionSpec.unpartitioned())
                .createWriterFunc(GenericParquetWriter::create).build();
        try (writer) {
            GenericRecord row = GenericRecord.create(schema);
            row.setField("id", 7L);
            row.setField("payload", "COPY exact Parquet value");
            writer.write(row);
        }
        return java.nio.file.Files.readAllBytes(file);
    }

    private static S3Client objects(URI endpoint) {
        return S3Client.builder().region(Region.US_EAST_1).endpointOverride(endpoint)
                .credentialsProvider(credentials())
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }

    private static URI minioEndpoint(GenericContainer<?> minio) {
        String host = minio.getHost();
        if (!"localhost".equals(host) && !"127.0.0.1".equals(host)) {
            throw new IllegalStateException("COPY source E2E requires a local Testcontainers endpoint");
        }
        return URI.create("http://" + host + ":" + minio.getMappedPort(9000));
    }

    private static final class Fixture implements AutoCloseable {
        private final PGSimpleDataSource source = new PGSimpleDataSource();
        private final PersistentGrantStore grants;
        private final JdbcStorageProfileRegistry profiles;
        private final JdbcCopySourceReadAdmissions admissions;
        private final UserIdentity identity = new UserIdentity("alice", Set.of(), Map.of(), Instant.now());
        private final CoordinatorExecutionAuthority processAuthority;
        private final HighAvailabilityCoordinationService coordination;
        private final OAuth2Authenticator oauth;
        private final AuthenticationManager authentication;
        private final Server server;
        private final org.pangrova.spi.coordination.ExecutionAnchor anchor;

        Fixture(BootstrapSecurityAuthority authority) throws Exception {
            var config = authority.config("coordinator-one");
            source.setURL(config.metadataJdbcConfig().jdbcUrl());
            source.setUser(config.metadataJdbcConfig().username());
            source.setPassword(config.metadataJdbcConfig().password());
            SchemaMigrationRunner.migrate(source);
            var roles = new PersistentRoleStore(source);
            roles.initSchema();
            var tenants = new JdbcTenantStore(source);
            tenants.createTenant(new Tenant("tenant-a", "Tenant A", TenantStatus.ACTIVE,
                    new ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0), List.of(), List.of(), null,
                    Map.of(), Instant.now(), "system"));
            tenants.addUserToTenant("tenant-a", "alice", TenantRole.MEMBER);
            roles.createRole(Role.of("reader"));
            roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
            grants = new PersistentGrantStore(source);
            profiles = new JdbcStorageProfileRegistry(source);
            var access = new DynamicRbacAccessControl(grants, roles,
                    new RowLevelSecurityFilter(new PersistentRowPolicyStore(source)),
                    new ColumnMaskingProcessor(new PersistentColumnMaskStore(source)));
            admissions = JdbcCopySourceReadAdmissions.forLoopbackTest(source, access);
            processAuthority = new CoordinatorExecutionAuthority(new JdbcExecutionAnchorStore(source),
                    "coordinator-one", "incarnation-one", Duration.ofMinutes(5));
            coordination = new HighAvailabilityCoordinationService(new JdbcDistributedLease(source));
            coordination.bindExecutionAuthority(processAuthority);
            coordination.start("coordinator-one");
            assertThat(coordination.acquireQueryOwnership("copy-http-execution", "coordinator-one")).isTrue();
            anchor = coordination.requireExecutionAnchor("copy-http-execution");
            oauth = new OAuth2Authenticator();
            oauth.init(config.securityConfig().authenticatorProperties());
            authentication = new AuthenticationManager(List.of(oauth));
            var resources = new ResourceConfig();
            resources.register(new AuthenticationFilter(authentication, authority.secret()));
            resources.register(new CopySourceReadResource(admissions, coordination));
            resources.register(CoordinatorExceptionMapper.class);
            resources.register(JacksonFeature.class);
            var handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
            handler.setContextPath("/");
            handler.addServlet(new ServletHolder(new ServletContainer(resources)), "/*");
            server = EmbeddedHttpServerFactory.create(0, handler);
            server.start();
        }

        void createProfile(String bucket, URI endpoint) {
            createProfile("incoming", bucket, endpoint.toString());
        }

        void createProfile(String name, String bucket, String endpoint) {
            assertThat(profiles.createIfAbsent(new StorageProfile(name, StorageType.S3,
                    bucket, "data/", endpoint, "us-east-1", Map.of(),
                    Map.of("path-style-access", "true")))).isTrue();
            grants.addGrant(PrivilegeGrant.of("reader", Privilege.READ_STORAGE_PROFILE,
                    "STORAGE_PROFILE", name, null, null, null));
        }

        JdbcCopySourceReadAdmissions.Scope scope(String incarnation, long revision,
                                                 String sourceUrl, String managedRoot,
                                                 String managedBackendIdentitySha256) {
            return scope("incoming", incarnation, revision, sourceUrl, managedRoot,
                    managedBackendIdentitySha256);
        }

        JdbcCopySourceReadAdmissions.Scope scope(String name, String incarnation, long revision,
                                                 String sourceUrl, String managedRoot) {
            return scope(name, incarnation, revision, sourceUrl, managedRoot, "a".repeat(64));
        }

        JdbcCopySourceReadAdmissions.Scope scope(String name, String incarnation, long revision,
                                                 String sourceUrl, String managedRoot,
                                                 String managedBackendIdentitySha256) {
            return new JdbcCopySourceReadAdmissions.Scope("tenant-a", "alice", name,
                    incarnation, revision, sourceUrl, managedRoot, managedBackendIdentitySha256,
                    "copy-http-execution",
                    "staging-one", anchor.ownerId(), anchor.ownerIncarnation(),
                    coordination.getCurrentEpoch(), anchor.fencingToken(),
                    System.currentTimeMillis() + 60_000);
        }

        CopySourceReadHttp.Scope wire(JdbcCopySourceReadAdmissions.Scope scope) {
            return new CopySourceReadHttp.Scope(scope.tenantId(), scope.principal(),
                    scope.profileName(), scope.profileIncarnationId(), scope.profileRevision(),
                    scope.canonicalSourceUrl(), scope.managedStorageRootUri(),
                    scope.managedBackendIdentitySha256(),
                    scope.executionId(), scope.stagingAttemptId(), scope.ownerId(),
                    scope.ownerIncarnation(), scope.controlEpoch(), scope.ownerFencingToken(),
                    scope.deadlineEpochMs());
        }

        String baseUri() {
            return "http://127.0.0.1:" + EmbeddedHttpServerFactory.getPort(server);
        }

        @Override public void close() throws Exception {
            Exception failure = null;
            try { server.stop(); } catch (Exception cleanup) { failure = cleanup; }
            try { authentication.close(); } catch (Exception cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
            try { coordination.close(); } catch (Exception cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
            try { processAuthority.close(); } catch (Exception cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
            if (failure != null) throw failure;
        }
    }
}
