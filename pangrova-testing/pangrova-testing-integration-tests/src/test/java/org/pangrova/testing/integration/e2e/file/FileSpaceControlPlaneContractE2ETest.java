package org.pangrova.testing.integration.e2e.file;

import org.pangrova.file.api.access.FileAccessGrant;
import org.pangrova.persistence.security.postgresql.audit.JdbcAuditEventWriter;
import org.pangrova.security.api.audit.AuditEvent;
import org.pangrova.security.api.audit.AuditEventQuery;
import org.pangrova.security.api.enums.AuditEventType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.file.access.FileSpaceAccessService;
import org.pangrova.file.access.FileGrantAudit;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;
import org.pangrova.filesystem.api.object.FileListing;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.enums.FileSpaceType;
import org.pangrova.filesystem.api.object.FileStat;
import org.pangrova.worker.file.FileSpaceFileResource;
import org.pangrova.worker.file.ObjectStoreAccessorFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the coordinator/worker contract end to end: the coordinator
 * authorizes a user and signs a credential-free descriptor, and the Worker
 * consumes that exact wire form to move bytes.
 */
@org.testcontainers.junit.jupiter.Testcontainers
class FileSpaceControlPlaneContractE2ETest {
    @org.testcontainers.junit.jupiter.Container
    private static final org.testcontainers.containers.PostgreSQLContainer<?> POSTGRES =
            new org.testcontainers.containers.PostgreSQLContainer<>("postgres:16-alpine");

    private static final String SECRET = "file-space-e2e-secret";
    private static final long NOW = 1_000_000L;
    private static final UserIdentity USER =
            new UserIdentity("alice", Set.of("analysts"), Map.of("tenant_id", "tenant-a"), Instant.EPOCH);

    @TempDir
    Path root;

    private final ObjectMapper mapper = JsonMapperFactory.create();
    private org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry spaces;
    private org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry profiles;
    private FileSpaceAccessService coordinator;
    private FileSpaceFileResource worker;
    private JdbcAuditEventWriter audit;

    @BeforeEach
    void setUp() throws Exception {
        var source = new org.postgresql.ds.PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        String schema = "file_issuer_" + UUID.randomUUID().toString().replace("-", "");
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        source.setCurrentSchema(schema);
        org.pangrova.persistence.jdbc.support.SchemaMigrationRunner.migrate(source);
        var tenants = new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(source);

        tenants.createTenant(new org.pangrova.security.api.tenant.Tenant("tenant-a", "File contract tests",
                org.pangrova.security.api.enums.TenantStatus.ACTIVE,
                new org.pangrova.security.api.tenant.ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0),
                List.of(), List.of(), "", Map.of(), Instant.EPOCH, "fixture"));
        tenants.addUserToTenant("tenant-a", "alice", org.pangrova.security.api.enums.TenantRole.MEMBER);
        spaces = new org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceRegistry(source);
        profiles = new org.pangrova.persistence.file.postgresql.profile.JdbcStorageProfileRegistry(source);
        assertThat(profiles.createIfAbsent(new StorageProfile("local", StorageType.LOCAL, root.toString(), "",
                "", "", Map.of(), Map.of()))).isTrue();
        for (FileSpaceType type : List.of(FileSpaceType.VOLUME, FileSpaceType.STAGE)) {
            spaces.create(new FileSpaceDefinition(type, "media", "raw", "landing", false,
                    "local", "", type == FileSpaceType.STAGE, "", "alice"), false);
        }
        var roles = new org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore(source);
        roles.createRole(org.pangrova.security.api.policy.Role.of("file_contract"));
        roles.grantRoleToUser(org.pangrova.security.api.policy.UserRoleAssignment.of("alice", "file_contract"));
        var grants = new org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore(source);
        for (var privilege : List.of(org.pangrova.security.api.enums.Privilege.READ_VOLUME,
                org.pangrova.security.api.enums.Privilege.WRITE_VOLUME)) {
            grants.addGrant(org.pangrova.security.api.policy.PrivilegeGrant.of("file_contract", privilege,
                    "VOLUME", "media", "raw", "landing", null));
        }
        for (var privilege : List.of(org.pangrova.security.api.enums.Privilege.USAGE_STAGE,
                org.pangrova.security.api.enums.Privilege.READ_STAGE, org.pangrova.security.api.enums.Privilege.WRITE_STAGE)) {
            grants.addGrant(org.pangrova.security.api.policy.PrivilegeGrant.of("file_contract", privilege,
                    "STAGE", "media", "raw", "landing", null));
        }
        var authorization = new org.pangrova.security.policy.authorization.DynamicRbacAccessControl(grants, roles,
                new org.pangrova.security.rowfilter.RowLevelSecurityFilter(
                        new org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore(source)),
                new org.pangrova.security.mask.ColumnMaskingProcessor(
                        new org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore(source)));
        audit = new JdbcAuditEventWriter(source);
        var context = new FileSpaceAccessService.FileSpaceAccessContext("local",
                Duration.ofMinutes(5), () -> NOW, () -> List.of("http://worker:8080"),
                new FileGrantAudit(audit), SECRET,
                new org.pangrova.persistence.file.postgresql.space.JdbcFileSpaceAuthority(source,
                        new org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore(source)));
        coordinator = new FileSpaceAccessService(spaces, profiles, authorization, context);
        worker = new FileSpaceFileResource(ObjectStoreAccessorFactory.discover(), SECRET, mapper,
                () -> NOW, audit);
    }

    @org.junit.jupiter.api.AfterEach
    void closeWorker() {
        if (worker != null) worker.close();
    }

    @Test
    void volumeTicketDrivesPutHeadRangeListAndDelete() throws IOException {
        FileAccessGrant write =
                grant("volumes", "images/a.bin", "write");
        assertThat(audit.query(AuditEventQuery.byPrincipal("alice")))
                .extracting(AuditEvent::eventType).containsExactly(AuditEventType.FILE_ACCESS_GRANTED);
        assertThat(write.toString()).doesNotContain(write.signature(), write.descriptor());
        FileStat written = worker.put(write.descriptor(), write.signature(), 5L, null,
                "text/plain", stream("hello"));
        assertThat(written.size()).isEqualTo(5);

        FileAccessGrant headGrant =
                grant("volumes", "images/a.bin", "head");
        FileStat head = worker.head(headGrant.descriptor(), headGrant.signature());
        assertThat(head.size()).isEqualTo(5);
        assertThat(head.providerVersion()).isEqualTo(written.providerVersion());

        FileAccessGrant read = grant("volumes", "images/a.bin", "read");
        Response range = worker.get(read.descriptor(), read.signature(), null, null,
                null, "bytes=1-3", '"' + head.etag() + '"', null);
        assertThat(range.getStatus()).isEqualTo(206);
        assertThat(range.getHeaderString("Content-Range")).isEqualTo("bytes 1-3/5");
        assertThat(audit.query(AuditEventQuery.byType(AuditEventType.FILE_DOWNLOADED))).isEmpty();
        assertThat(content(range)).isEqualTo("ell");
        assertThat(audit.query(AuditEventQuery.byType(AuditEventType.FILE_DOWNLOADED)))
                .singleElement().satisfies(event -> assertThat(event.bytesRead()).isEqualTo(3));
        try (var response = worker.headObject(read.descriptor(), read.signature(), null, null)) {
            assertThat(response.getStatus()).isEqualTo(200);
        }
        try (var aborted = worker.get(read.descriptor(), read.signature(), null, null, null, null, null, null)) {
            var stream = (StreamingOutput) aborted.getEntity();
            assertThatThrownBy(() -> stream.write(new java.io.OutputStream() {
                @Override public void write(int value) throws IOException { throw new IOException("client disconnected"); }
            })).isInstanceOf(IOException.class);
        }
        assertThatThrownBy(() -> worker.get(read.descriptor(), read.signature(), null, null,
                null, null, "\"stale\"", null))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED));

        FileAccessGrant list = grant("volumes", "images", "list");
        FileListing listing = worker.list(new FileSpaceFileResource.ListRequest(
                list.descriptor(), list.signature(), null, 10));
        assertThat(listing.files()).extracting(FileStat::path).containsExactly("images/a.bin");

        FileAccessGrant delete =
                grant("volumes", "images/a.bin", "delete");
        assertThatThrownBy(() -> worker.delete(delete.descriptor(), delete.signature(),
                new FileSpaceFileResource.DeleteRequest("stale-version")))
                .isInstanceOf(PangrovaException.class);
        assertThat(audit.query(AuditEventQuery.byType(AuditEventType.FILE_DELETED))).isEmpty();
        Response removed = worker.delete(delete.descriptor(), delete.signature(),
                new FileSpaceFileResource.DeleteRequest(head.providerVersion()));
        assertThat(removed.getStatus()).isEqualTo(204);

        FileAccessGrant relist = grant("volumes", "images", "list");
        assertThat(worker.list(new FileSpaceFileResource.ListRequest(
                relist.descriptor(), relist.signature(), null, 10)).files()).isEmpty();
        var events = audit.query(AuditEventQuery.byPrincipal("alice"));
        var spaceId = spaces.find(FileSpaceType.VOLUME, "media", "raw", "landing").orElseThrow().id();
        assertThat(events).allSatisfy(event -> {
            assertThat(event.properties()).containsEntry("tenant_id", "tenant-a")
                    .containsEntry("space_id", spaceId.toString());
            assertThat(event.toString()).doesNotContain(write.signature(), write.descriptor());
        });
        assertThat(events.stream().filter(event -> event.eventType() == AuditEventType.FILE_ACCESS_GRANTED))
                .hasSize(6);
        assertThat(events.stream().map(AuditEvent::eventType)
                .filter(type -> type != AuditEventType.FILE_ACCESS_GRANTED))
                .containsExactlyInAnyOrder(AuditEventType.FILE_UPLOADED, AuditEventType.FILE_METADATA_READ,
                        AuditEventType.FILE_METADATA_READ, AuditEventType.FILE_DOWNLOADED, AuditEventType.FILE_LISTED,
                        AuditEventType.FILE_DELETED, AuditEventType.FILE_LISTED);
        assertThatThrownBy(() -> grant("volumes", "images", "peek")).isInstanceOf(PangrovaException.class);
        assertThat(audit.query(AuditEventQuery.byPrincipal("alice"))).hasSameSizeAs(events);
    }

    @Test
    void stageTicketRootsUnderStagesAndCannotBeUsedForAnotherOperation() throws IOException {
        FileAccessGrant write = grant("stages", "events/a.csv", "write");
        var stage = spaces.find(FileSpaceType.STAGE, "media", "raw", "landing").orElseThrow();
        assertThat(descriptorFields(write)).containsEntry("baseKey", "managed/stages/" + stage.id())
                .containsEntry("path", "events/a.csv")
                .containsEntry("type", "LOCAL");

        FileAccessGrant read = grant("stages", "events/a.csv", "read");
        assertThatThrownBy(() -> worker.put(read.descriptor(), read.signature(), 1L, null,
                "text/plain", stream("x")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void ticketStopsWorkingWhenItsDescriptorExpires() {
        FileAccessGrant write = grant("volumes", "images/a.bin", "write");
        try (var later = new FileSpaceFileResource(ObjectStoreAccessorFactory.discover(), SECRET, mapper,
                () -> NOW + Duration.ofMinutes(6).toMillis(), audit)) {
            assertThatThrownBy(() -> later.put(write.descriptor(), write.signature(), 1L, null,
                    "text/plain", stream("x")))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
            assertThat(audit.query(AuditEventQuery.byType(AuditEventType.FILE_UPLOADED))).isEmpty();
        }
    }

    private FileAccessGrant grant(String kind, String path,
            String operation) {
        return coordinator.grant(USER, kind, "media", "raw", "landing", path, operation);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> descriptorFields(
            FileAccessGrant grant) throws IOException {
        return mapper.readValue(Base64.getUrlDecoder().decode(grant.descriptor()), Map.class);
    }

    private static String content(Response response) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ((StreamingOutput) response.getEntity()).write(out);
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }

    private static InputStream stream(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

}
