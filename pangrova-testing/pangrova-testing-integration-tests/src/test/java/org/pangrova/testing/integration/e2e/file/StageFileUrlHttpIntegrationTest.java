package org.pangrova.testing.integration.e2e.file;

import org.pangrova.file.api.access.FileAccessGrant;

import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
import org.postgresql.ds.PGSimpleDataSource;
import org.pangrova.transport.http.EmbeddedHttpServerFactory;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.file.access.FileSpaceAccessService;
import org.pangrova.file.access.FileUrlService;
import org.pangrova.server.coordinator.http.endpoint.file.StageFileUrlService;
import org.pangrova.file.worker.WorkerFileClient;
import org.pangrova.server.coordinator.http.endpoint.AuthenticationFilter;
import org.pangrova.server.coordinator.http.endpoint.CoordinatorExceptionMapper;
import org.pangrova.server.coordinator.http.endpoint.file.FileSpaceFilesResource;
import org.pangrova.file.access.FileGrantAudit;
import org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry;
import org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry;
import org.pangrova.persistence.jdbc.support.SchemaMigrationRunner;
import org.pangrova.security.authentication.AuthenticationManager;
import org.pangrova.security.api.authentication.AuthenticationRequest;
import org.pangrova.security.api.authentication.AuthenticationResult;
import org.pangrova.security.api.authentication.Authenticator;
import org.pangrova.security.policy.authorization.DynamicRbacAccessControl;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.persistence.security.postgresql.transaction.AuthorizationTransactionGate;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.space.FileSpaceLayout;
import org.pangrova.file.api.enums.FileSpaceType;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.worker.file.FileSpaceFileResource;
import org.pangrova.worker.file.ObjectStoreAccessorFactory;
import org.pangrova.worker.http.endpoint.InternalAuthFilter;
import org.pangrova.worker.http.endpoint.WorkerExceptionMapper;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual HTTP authentication, durable RBAC, Stage identity and Worker byte transfer. */
@E2ETest
@Testcontainers
class StageFileUrlHttpIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");
    private static final String SECRET = "stage-url-integration-secret";
    private static final HttpClient CLIENT = HttpClient.newBuilder().proxy(ProxySelector.of(null))
            .connectTimeout(Duration.ofSeconds(10)).build();
    @TempDir
    Path root;
    private PGSimpleDataSource source;
    private JdbcFileSpaceRegistry spaces;
    private JdbcGrantStore grants;
    private FileSpace stage;
    private Server coordinator;
    private Server worker;
    private AuthenticationManager authentication;
    private ObjectStoreAccessorFactory accessors;
    private URI base;
    private StorageProfile profile;

    @BeforeEach
    void setup() throws Exception {
        source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        String schema = "stage_url_" + UUID.randomUUID().toString().replace("-", "");
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        source.setCurrentSchema(schema);
        SchemaMigrationRunner.migrate(source);
        var tenants = new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(source);

        tenants.createTenant(new org.pangrova.security.api.tenant.Tenant("tenant-a", "HTTP file tests",
                org.pangrova.security.api.enums.TenantStatus.ACTIVE,
                new org.pangrova.security.api.tenant.ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0),
                List.of(), List.of(), "", Map.of(), Instant.EPOCH, "fixture"));
        for (String principal : List.of("alice", "bob")) {
            tenants.addUserToTenant("tenant-a", principal, org.pangrova.security.api.enums.TenantRole.MEMBER);
        }
        grants = new JdbcGrantStore(source);
        var roles = new JdbcRoleStore(source);
        roles.createRole(Role.of("reader"));
        roles.grantRoleToUser(UserRoleAssignment.of("alice", "reader"));
        var rbac = new DynamicRbacAccessControl(grants, roles,
                new RowLevelSecurityFilter(new JdbcRowPolicyStore(source)),
                new ColumnMaskingProcessor(new JdbcColumnMaskStore(source)));
        spaces = new JdbcFileSpaceRegistry(source);
        stage = spaces.create(definition("landing", false), false);
        grant("landing", Privilege.READ_STAGE);
        profile = new StorageProfile("local", StorageType.LOCAL, root.toString(), "",
                "", "", Map.of(), Map.of());
        var profiles = new JdbcStorageProfileRegistry(source);
        assertThat(profiles.createIfAbsent(profile)).isTrue();
        accessors = ObjectStoreAccessorFactory.discover();
        worker = start(new ResourceConfig().register(new FileSpaceFileResource(accessors, SECRET, event -> {}))
                .register(new InternalAuthFilter(SECRET)).register(WorkerExceptionMapper.class));
        var context = FileSpaceAccessService.FileSpaceAccessContext.of("local",
                () -> List.of(base(worker).toString()), new FileGrantAudit(event -> {}), SECRET,
                new org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceAuthority(source,
                        new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(source)));
        var access = new FileSpaceAccessService(spaces, profiles, rbac, context);
        authentication = new AuthenticationManager(List.of(new TestAuthenticator()));
        coordinator = start(new ResourceConfig().register(new FileSpaceFilesResource(access,
                new FileUrlService(access, new WorkerFileClient(CLIENT, SECRET)),
                new StageFileUrlService(access)))
                .register(new AuthenticationFilter(authentication, SECRET))
                .register(CoordinatorExceptionMapper.class));
        base = base(coordinator);
    }

    @AfterEach
    void stop() throws Exception {
        try {
            if (coordinator != null) coordinator.stop();
        } finally {
            try {
                if (worker != null) worker.stop();
            } finally {
                if (accessors != null) accessors.close();
                if (authentication != null) authentication.close();
            }
        }
    }

    @Test
    void stableLinkReadsWorkerBytesAndPreservesRangeAndConditionalRequests() throws Exception {
        put(stage, "images/测试 a+b.txt", "0123456789");
        URI link = build("landing", "images/测试 a+b.txt");
        assertThat(link.toString()).contains(stage.id().toString()).doesNotContain("signature", "descriptor");
        var redirect = get(link, "alice");
        assertThat(redirect.statusCode()).isEqualTo(307);
        assertThat(redirect.body()).isEmpty();
        assertThat(redirect.headers().firstValue("Cache-Control")).contains("no-store");
        assertThat(redirect.headers().firstValue("Referrer-Policy")).contains("no-referrer");
        URI transfer = URI.create(redirect.headers().firstValue("Location").orElseThrow());
        var range = CLIENT.send(HttpRequest.newBuilder(transfer).header("Range", "bytes=2-5").GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(range.statusCode()).isEqualTo(206);
        assertThat(range.body()).isEqualTo("2345");
        var stale = CLIENT.send(HttpRequest.newBuilder(transfer).header("If-Match", "\"stale\"").GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(stale.statusCode()).isEqualTo(412);
        var head = CLIENT.send(HttpRequest.newBuilder(link).header("Authorization", "Bearer alice-token")
                .method("HEAD", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(head.statusCode()).isEqualTo(307);
        assertThat(head.body()).isEmpty();
    }

    @Test
    void authenticationAndRevocationAreCheckedEveryTime() throws Exception {
        URI link = build("landing", "a.txt");
        assertThat(get(link, null).statusCode()).isEqualTo(401);
        assertThat(get(link, "bob").statusCode()).isEqualTo(403);
        var internal = CLIENT.send(HttpRequest.newBuilder(link).header("Authorization", "Bearer " + SECRET)
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(internal.statusCode()).isEqualTo(403);
        assertThat(get(link, "alice").statusCode()).isEqualTo(307);
        grants.revokeGrants("reader", Privilege.READ_STAGE, "media", "raw", "landing");
        assertThat(get(link, "alice").statusCode()).isEqualTo(403);
        assertThat(post("landing", "a.txt", "alice").statusCode()).isEqualTo(403);
    }

    @Test
    void renamedStageRetainsItsLinkAndDropNeverRebindsToSameName() throws Exception {
        put(stage, "a.txt", "original");
        URI link = build("landing", "a.txt");
        var renamed = spaces.rename(FileSpaceType.STAGE, "media", "raw", "landing", "renamed");
        assertThat(renamed.id()).isEqualTo(stage.id());
        assertThat(spaces.find(stage.id())).contains(renamed);
        assertThat(download(link)).isEqualTo("original");
        var replacement = spaces.create(definition("landing", false), false);
        grant("landing", Privilege.READ_STAGE);
        put(replacement, "a.txt", "replacement");
        assertThat(download(link)).isEqualTo("original");
        assertThat(download(build("landing", "a.txt"))).isEqualTo("replacement");
        spaces.drop(FileSpaceType.STAGE, "media", "raw", "renamed", false);
        assertThat(spaces.find(stage.id())).isEmpty();
        assertThat(get(link, "alice").statusCode()).isEqualTo(404);
        assertThat(download(build("landing", "a.txt"))).isEqualTo("replacement");
    }

    @Test
    void externalStageNeedsUsageAndVolumeIdsCannotEnterStageRoute() throws Exception {
        FileSpace external = spaces.create(definition("external", true), false);
        grant("external", Privilege.USAGE_STAGE);
        Files.createDirectories(root.resolve("external"));
        Files.writeString(root.resolve("external/a.txt"), "external bytes");
        assertThat(download(build("external", "a.txt"))).isEqualTo("external bytes");
        grants.revokeGrants("reader", Privilege.USAGE_STAGE, "media", "raw", "external");
        grant("external", Privilege.READ_STAGE);
        assertThat(post("external", "a.txt", "alice").statusCode()).isEqualTo(403);
        assertThat(get(base.resolve("v1/stage-files/" + external.id() + "?path=a.txt"), "alice")
                .statusCode()).isEqualTo(403);
        var volume = spaces.create(new FileSpaceDefinition(FileSpaceType.VOLUME, "media", "raw", "volume",
                false, "local", "", false, "", "alice"), false);
        assertThat(get(base.resolve("v1/stage-files/" + volume.id() + "?path=a.txt"), "alice")
                .statusCode()).isEqualTo(404);
    }

    @Test
    void realGrantsCannotResumeAnotherSubjectsOrRecreatedNamespacesMultipartSession() throws Exception {
        spaces.create(definition("external", true), false);
        for (String name : List.of("external")) {
            for (Privilege privilege : List.of(Privilege.USAGE_STAGE, Privilege.READ_STAGE, Privilege.WRITE_STAGE)) {
                grant(name, privilege);
            }
        }
        new JdbcRoleStore(source).grantRoleToUser(UserRoleAssignment.of("bob", "reader"));
        var original = accessGrant("external", "alice", "write");
        var mapper = JsonMapperFactory.create();
        URI endpoint = base(worker).resolve("v1/file-space/");
        var began = workerPost(endpoint.resolve("multipart/begin"),
                new FileSpaceFileResource.BeginMultipartRequest(original.descriptor(), original.signature(), null));
        assertThat(began.statusCode()).as(began.body()).isEqualTo(200);
        var session = mapper.readValue(began.body(), FileSpaceFileResource.MultipartSession.class);
        var denied = multipartPart(endpoint, accessGrant("external", "bob", "write"), session.uploadId());
        assertThat(denied.statusCode()).as(denied.body()).isEqualTo(409);
        spaces.rename(FileSpaceType.STAGE, "media", "raw", "external", "renamed_upload");
        var renewed = accessGrant("renamed_upload", "alice", "write");
        var resumed = multipartPart(endpoint, renewed, session.uploadId());
        assertThat(resumed.statusCode()).as(resumed.body()).isEqualTo(200);
        var part = mapper.readValue(resumed.body(), org.pangrova.filesystem.api.object.UploadedPart.class);
        spaces.drop(FileSpaceType.STAGE, "media", "raw", "renamed_upload", false);
        spaces.create(definition("external", true), false);
        for (Privilege privilege : List.of(Privilege.USAGE_STAGE, Privilege.READ_STAGE, Privilege.WRITE_STAGE)) {
            grant("external", privilege);
        }
        var replacement = multipartPart(endpoint, accessGrant("external", "alice", "write"), session.uploadId());
        assertThat(replacement.statusCode()).as(replacement.body()).isEqualTo(409);
        // A previously issued bearer ticket retains its original bounded lifetime.
        var completed = workerPost(endpoint.resolve("multipart/complete"),
                new FileSpaceFileResource.CompleteMultipartRequest(original.descriptor(), original.signature(),
                        session.uploadId(), List.of(new FileSpaceFileResource.MultipartPart(
                                part.partNumber(), part.etag(), part.size()))));
        assertThat(completed.statusCode()).as(completed.body()).isEqualTo(200);
        var read = accessGrant("external", "alice", "read");
        var bytes = CLIENT.send(HttpRequest.newBuilder(URI.create(endpoint + "object?descriptor="
                        + read.descriptor() + "&signature=" + read.signature())).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(bytes.statusCode()).as(bytes.body()).isEqualTo(200);
        assertThat(bytes.body()).isEqualTo("verified bytes");
    }

    private FileAccessGrant accessGrant(String name, String user, String operation)
            throws Exception {
        var body = new FileSpaceFilesResource.AccessRequest("scoped.bin", operation);
        var response = CLIENT.send(HttpRequest.newBuilder(base.resolve("v1/stages/media/raw/" + name + "/files/access"))
                .header("Content-Type", "application/json").header("Authorization", "Bearer " + user + "-token")
                .POST(HttpRequest.BodyPublishers.ofString(JsonMapperFactory.create().writeValueAsString(body))).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return JsonMapperFactory.create().readValue(response.body(), FileAccessGrant.class);
    }

    private HttpResponse<String> multipartPart(URI endpoint, FileAccessGrant grant,
            String uploadId) throws Exception {
        return CLIENT.send(HttpRequest.newBuilder(URI.create(endpoint + "multipart/part?descriptor="
                        + grant.descriptor() + "&signature=" + grant.signature() + "&uploadId=" + uploadId
                        + "&partNumber=1"))
                .header("Content-Type", "application/octet-stream")
                .PUT(HttpRequest.BodyPublishers.ofString("verified bytes")).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> workerPost(URI endpoint, Object body) throws Exception {
        return CLIENT.send(HttpRequest.newBuilder(endpoint).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JsonMapperFactory.create().writeValueAsString(body))).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void unsafePathsAreRejectedBeforeRedirect() throws Exception {
        for (String path : List.of("", "../secret", "a/../secret", "a\\secret", "%2e%2e/secret")) {
            assertThat(post("landing", path, "alice").statusCode()).isEqualTo(400);
        }
        assertThat(get(base.resolve("v1/stage-files/" + stage.id() + "?path=../secret"), "alice")
                .statusCode()).isEqualTo(400);
    }

    @Test
    void concurrentRevocationCompletesBeforeQueuedLinkCanIssueTicket() throws Exception {
        URI link = build("landing", "a.txt");
        try (var mutation = source.getConnection()) {
            mutation.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(mutation);
            try (var revoke = mutation.prepareStatement("DELETE FROM security_grants WHERE role_name='reader'")) {
                revoke.executeUpdate();
            }
            var pending = CLIENT.sendAsync(HttpRequest.newBuilder(link)
                    .header("Authorization", "Bearer alice-token").GET().build(), HttpResponse.BodyHandlers.ofString());
            await().atMost(Duration.ofSeconds(4)).until(this::waitingForAuthority);
            assertThat(pending.isDone()).isFalse();
            mutation.commit();
            assertThat(pending.get(10, TimeUnit.SECONDS).statusCode()).isEqualTo(403);
        }
    }

    @Test
    void ordinaryFileTicketsWaitForTenantRevocationAndDoNotUseOldAuthenticationClaims() throws Exception {
        URI link = build("landing", "scoped.bin");
        var body = JsonMapperFactory.create().writeValueAsString(
                new FileSpaceFilesResource.AccessRequest("scoped.bin", "read"));
        var request = HttpRequest.newBuilder(base.resolve("v1/stages/media/raw/landing/files/access"))
                .header("Content-Type", "application/json").header("Authorization", "Bearer alice-token")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        grant("landing", Privilege.USAGE_STAGE);
        assertThat(CLIENT.send(request, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
        try (var mutation = source.getConnection()) {
            mutation.setAutoCommit(false);
            AuthorizationTransactionGate.lockMutation(mutation);
            try (var remove = mutation.prepareStatement(
                    "DELETE FROM tenant_user_memberships WHERE user_principal='alice'")) {
                remove.executeUpdate();
            }
            var pending = CLIENT.sendAsync(request, HttpResponse.BodyHandlers.ofString());
            await().atMost(Duration.ofSeconds(4)).until(this::waitingForAuthority);
            assertThat(pending.isDone()).isFalse();
            mutation.commit();
            assertThat(pending.get(10, TimeUnit.SECONDS).statusCode()).isEqualTo(403);
        }
        assertThat(get(link, "alice").statusCode()).isEqualTo(403);
    }

    private boolean waitingForAuthority() throws Exception {
        try (var connection = source.getConnection(); var query = connection.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event='advisory' "
                        + "AND query LIKE 'SELECT pg_advisory_xact_lock_shared%')"); var row = query.executeQuery()) {
            row.next();
            return row.getBoolean(1);
        }
    }

    private void grant(String name, Privilege privilege) {
        grants.addGrant(PrivilegeGrant.of("reader", privilege, "STAGE", "media", "raw", name, null));
    }

    private FileSpaceDefinition definition(String name, boolean external) {
        return new FileSpaceDefinition(FileSpaceType.STAGE, "media", "raw", name, external,
                "local", external ? root.resolve("external").toUri().toString() : "", false, "", "alice");
    }

    private void put(FileSpace space, String path, String bytes) throws Exception {
        Path file = root.resolve(FileSpaceLayout.managedKey(space, profile)).resolve(path);
        Files.createDirectories(file.getParent());
        Files.writeString(file, bytes);
    }

    private URI build(String name, String path) throws Exception {
        var response = post(name, path, "alice");
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return URI.create(JsonMapperFactory.create().readTree(response.body()).get("url").asText());
    }

    private HttpResponse<String> post(String name, String path, String user) throws Exception {
        return CLIENT.send(HttpRequest.newBuilder(base.resolve("v1/stages/media/raw/" + name + "/files/stage-url"))
                .header("Content-Type", "application/json").header("Authorization", "Bearer " + user + "-token")
                .POST(HttpRequest.BodyPublishers.ofString(JsonMapperFactory.create()
                        .writeValueAsString(new FileSpaceFilesResource.StageUrlRequest(path)))).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> get(URI uri, String user) throws Exception {
        var request = HttpRequest.newBuilder(uri).GET();
        if (user != null) request.header("Authorization", "Bearer " + user + "-token");
        return CLIENT.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String download(URI link) throws Exception {
        var redirect = get(link, "alice");
        assertThat(redirect.statusCode()).as(redirect.body()).isEqualTo(307);
        var response = get(URI.create(redirect.headers().firstValue("Location").orElseThrow()), null);
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return response.body();
    }

    private static Server start(ResourceConfig resources) throws Exception {
        resources.register(JacksonFeature.class);
        var handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(new ServletContainer(resources)), "/*");
        var server = EmbeddedHttpServerFactory.create(0, handler);
        server.start();
        return server;
    }

    private static URI base(Server server) {
        return URI.create("http://localhost:" + EmbeddedHttpServerFactory.getPort(server) + "/");
    }

    /** Deterministic identity provider; the production filter and durable authorization remain under test. */
    private static final class TestAuthenticator implements Authenticator {
        @Override public String getName() { return "test-identities"; }
        @Override public void init(Map<String, String> config) { }
        @Override public boolean supports(AuthenticationRequest request) { return true; }
        @Override public boolean isHealthy() { return true; }
        @Override public void close() { }
        @Override public AuthenticationResult authenticate(AuthenticationRequest request) {
            for (String name : List.of("alice", "bob")) {
                if ((name + "-token").equals(request.token())) {
                    return AuthenticationResult.success(new UserIdentity(name, Set.of(), Map.of(), Instant.EPOCH));
                }
            }
            return AuthenticationResult.failure("Unknown test credential");
        }
    }
}
