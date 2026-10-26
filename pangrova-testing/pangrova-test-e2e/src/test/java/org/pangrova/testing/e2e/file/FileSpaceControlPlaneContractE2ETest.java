package org.pangrova.testing.e2e.file;

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
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.file.FileSpaceAccessService;
import org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageType;
import org.pangrova.spi.storage.file.FileListing;
import org.pangrova.spi.storage.file.FileSpaceDefinition;
import org.pangrova.spi.storage.file.FileSpaceType;
import org.pangrova.spi.storage.file.FileStat;
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
    private org.pangrova.metadata.file.JdbcFileSpaceRegistry spaces;
    private org.pangrova.metadata.storage.JdbcStorageProfileRegistry profiles;
    private FileSpaceAccessService coordinator;
    private FileSpaceFileResource worker;

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
        org.pangrova.metadata.store.SchemaMigrationRunner.migrate(source);
        var tenants = new org.pangrova.metadata.tenant.JdbcTenantStore(source);
        tenants.initSchema();
        tenants.createTenant(new org.pangrova.security.tenant.model.Tenant("tenant-a", "File contract tests",
                org.pangrova.security.tenant.model.TenantStatus.ACTIVE,
                new org.pangrova.security.tenant.model.ResourceQuota(0, 0, 0, 0, 0, 0, 0, 0),
                List.of(), List.of(), "", Map.of(), Instant.EPOCH, "fixture"));
        tenants.addUserToTenant("tenant-a", "alice", org.pangrova.security.tenant.model.TenantRole.MEMBER);
        spaces = new org.pangrova.metadata.file.JdbcFileSpaceRegistry(source);
        profiles = new org.pangrova.metadata.storage.JdbcStorageProfileRegistry(source);
        assertThat(profiles.createIfAbsent(new StorageProfile("local", StorageType.LOCAL, root.toString(), "",
                "", "", Map.of(), Map.of()))).isTrue();
        for (FileSpaceType type : List.of(FileSpaceType.VOLUME, FileSpaceType.STAGE)) {
            spaces.create(new FileSpaceDefinition(type, "media", "raw", "landing", false,
                    "local", "", type == FileSpaceType.STAGE, "", "alice"), false);
        }
        var roles = new org.pangrova.security.store.PersistentRoleStore(source);
        roles.initSchema();
        roles.createRole(org.pangrova.security.authz.Role.of("file_contract"));
        roles.grantRoleToUser(org.pangrova.security.authz.UserRoleAssignment.of("alice", "file_contract"));
        var grants = new org.pangrova.security.store.PersistentGrantStore(source);
        for (var privilege : List.of(org.pangrova.security.authz.Privilege.READ_VOLUME,
                org.pangrova.security.authz.Privilege.WRITE_VOLUME)) {
            grants.addGrant(org.pangrova.security.authz.PrivilegeGrant.of("file_contract", privilege,
                    "VOLUME", "media", "raw", "landing", null));
        }
        for (var privilege : List.of(org.pangrova.security.authz.Privilege.USAGE_STAGE,
                org.pangrova.security.authz.Privilege.READ_STAGE, org.pangrova.security.authz.Privilege.WRITE_STAGE)) {
            grants.addGrant(org.pangrova.security.authz.PrivilegeGrant.of("file_contract", privilege,
                    "STAGE", "media", "raw", "landing", null));
        }
        var authorization = new org.pangrova.security.authz.DynamicRbacAccessControl(grants, roles,
                new org.pangrova.security.rowfilter.RowLevelSecurityFilter(
                        new org.pangrova.security.store.PersistentRowPolicyStore(source)),
                new org.pangrova.security.mask.ColumnMaskingProcessor(
                        new org.pangrova.security.store.PersistentColumnMaskStore(source)));
        var context = new FileSpaceAccessService.FileSpaceAccessContext("local",
                Duration.ofMinutes(5), () -> NOW, () -> List.of("http://worker:8080"),
                new FileSpaceAuditEmitter(null), SECRET, new org.pangrova.metadata.file.JdbcFileSpaceAuthority(source));
        coordinator = new FileSpaceAccessService(spaces, profiles, authorization, context);
        worker = new FileSpaceFileResource(new ObjectStoreAccessorFactory(), SECRET, mapper,
                () -> NOW);
    }

    @Test
    void volumeTicketDrivesPutHeadRangeListAndDelete() throws IOException {
        FileSpaceAccessService.FileAccessGrant write =
                grant("volumes", "images/a.bin", "write");
        FileStat written = worker.put(write.descriptor(), write.signature(), 5L, null,
                "text/plain", stream("hello"));
        assertThat(written.size()).isEqualTo(5);

        FileSpaceAccessService.FileAccessGrant headGrant =
                grant("volumes", "images/a.bin", "head");
        FileStat head = worker.head(headGrant.descriptor(), headGrant.signature());
        assertThat(head.size()).isEqualTo(5);
        assertThat(head.providerVersion()).isEqualTo(written.providerVersion());

        FileSpaceAccessService.FileAccessGrant read = grant("volumes", "images/a.bin", "read");
        Response range = worker.get(read.descriptor(), read.signature(), null, null,
                null, "bytes=1-3", '"' + head.etag() + '"', null);
        assertThat(range.getStatus()).isEqualTo(206);
        assertThat(range.getHeaderString("Content-Range")).isEqualTo("bytes 1-3/5");
        assertThat(content(range)).isEqualTo("ell");
        assertThatThrownBy(() -> worker.get(read.descriptor(), read.signature(), null, null,
                null, null, "\"stale\"", null))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_OBJECT_PRECONDITION_FAILED));

        FileSpaceAccessService.FileAccessGrant list = grant("volumes", "images", "list");
        FileListing listing = worker.list(new FileSpaceFileResource.ListRequest(
                list.descriptor(), list.signature(), null, 10));
        assertThat(listing.files()).extracting(FileStat::path).containsExactly("images/a.bin");

        FileSpaceAccessService.FileAccessGrant delete =
                grant("volumes", "images/a.bin", "delete");
        Response removed = worker.delete(delete.descriptor(), delete.signature(),
                new FileSpaceFileResource.DeleteRequest(head.providerVersion()));
        assertThat(removed.getStatus()).isEqualTo(204);

        FileSpaceAccessService.FileAccessGrant relist = grant("volumes", "images", "list");
        assertThat(worker.list(new FileSpaceFileResource.ListRequest(
                relist.descriptor(), relist.signature(), null, 10)).files()).isEmpty();
    }

    @Test
    void stageTicketRootsUnderStagesAndCannotBeUsedForAnotherOperation() throws IOException {
        FileSpaceAccessService.FileAccessGrant write = grant("stages", "events/a.csv", "write");
        var stage = spaces.find(FileSpaceType.STAGE, "media", "raw", "landing").orElseThrow();
        assertThat(descriptorFields(write)).containsEntry("baseKey", "managed/stages/" + stage.id())
                .containsEntry("path", "events/a.csv")
                .containsEntry("type", "LOCAL");

        FileSpaceAccessService.FileAccessGrant read = grant("stages", "events/a.csv", "read");
        assertThatThrownBy(() -> worker.put(read.descriptor(), read.signature(), 1L, null,
                "text/plain", stream("x")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void ticketStopsWorkingWhenItsDescriptorExpires() {
        FileSpaceAccessService.FileAccessGrant write = grant("volumes", "images/a.bin", "write");
        FileSpaceFileResource later = new FileSpaceFileResource(
                new ObjectStoreAccessorFactory(), SECRET, mapper,
                () -> NOW + Duration.ofMinutes(6).toMillis());

        assertThatThrownBy(() -> later.put(write.descriptor(), write.signature(), 1L, null,
                "text/plain", stream("x")))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    private FileSpaceAccessService.FileAccessGrant grant(String kind, String path,
            String operation) {
        return coordinator.grant(USER, kind, "media", "raw", "landing", path, operation);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> descriptorFields(
            FileSpaceAccessService.FileAccessGrant grant) throws IOException {
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
