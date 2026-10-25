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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.pangrova.spi.security.FileSpaceAccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageProfileRegistry;
import org.pangrova.spi.storage.StorageProfileRevision;
import org.pangrova.spi.storage.StorageType;
import org.pangrova.spi.storage.file.FileListing;
import org.pangrova.spi.storage.file.FileSpace;
import org.pangrova.spi.storage.file.FileSpaceDefinition;
import org.pangrova.spi.storage.file.FileSpaceMutation;
import org.pangrova.spi.storage.file.FileSpaceRegistry;
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
class FileSpaceControlPlaneContractE2ETest {

    private static final String SECRET = "file-space-e2e-secret";
    private static final long NOW = 1_000_000L;
    private static final UserIdentity USER =
            new UserIdentity("alice", Set.of("analysts"), Map.of(), Instant.EPOCH);

    @TempDir
    Path root;

    private final ObjectMapper mapper = JsonMapperFactory.create();
    private final FakeSpaces spaces = new FakeSpaces();
    private final FakeProfiles profiles = new FakeProfiles();
    private FileSpaceAccessService coordinator;
    private FileSpaceFileResource worker;

    @BeforeEach
    void setUp() {
        spaces.put(new FileSpace(UUID.randomUUID(), FileSpaceType.VOLUME, "media", "raw", "landing", false,
                "local", "", "volumes/media/raw/landing", false, "", "alice", Instant.EPOCH, Instant.EPOCH));
        spaces.put(new FileSpace(UUID.randomUUID(), FileSpaceType.STAGE, "media", "raw", "landing", false,
                "local", "", "stages/media/raw/landing", true, "", "alice", Instant.EPOCH, Instant.EPOCH));
        profiles.put(new StorageProfile("local", StorageType.LOCAL, root.toString(), "",
                "", "", Map.of(), Map.of()));

        var context = new FileSpaceAccessService.FileSpaceAccessContext("local",
                Duration.ofMinutes(5), () -> NOW, () -> List.of("http://worker:8080"),
                new FileSpaceAuditEmitter(null), SECRET);
        coordinator = new FileSpaceAccessService(spaces, profiles, new AllowAll(), context);
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
        assertThat(descriptorFields(write)).containsEntry("baseKey", "stages/media/raw/landing")
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

    private static final class AllowAll implements FileSpaceAccessControl {

        @Override
        public void checkCanReadVolume(UserIdentity user, String catalog, String schema,
                String name) {
        }

        @Override
        public void checkCanWriteVolume(UserIdentity user, String catalog, String schema,
                String name) {
        }

        @Override
        public void checkCanUseStage(UserIdentity user, String catalog, String schema,
                String name) {
        }

        @Override
        public void checkCanReadStage(UserIdentity user, String catalog, String schema,
                String name) {
        }

        @Override
        public void checkCanWriteStage(UserIdentity user, String catalog, String schema,
                String name) {
        }
    }

    private static final class FakeSpaces implements FileSpaceRegistry {
        private final Map<String, FileSpace> byName = new HashMap<>();

        void put(FileSpace space) {
            byName.put(key(space.type(), space.catalog(), space.schema(), space.name()), space);
        }

        private static String key(FileSpaceType type, String catalog, String schema,
                String name) {
            return type + "/" + catalog + "/" + schema + "/" + name;
        }

        @Override
        public FileSpace create(FileSpaceDefinition definition, boolean ifNotExists) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileSpace mutate(FileSpaceType type, String catalog, String schema, String name,
                FileSpaceMutation mutation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileSpace rename(FileSpaceType type, String catalog, String schema, String name,
                String newName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<FileSpace> find(java.util.UUID id) {
            return byName.values().stream().filter(space -> space.id().equals(id)).findFirst();
        }

        @Override
        public Optional<FileSpace> find(FileSpaceType type, String catalog, String schema,
                String name) {
            return Optional.ofNullable(byName.get(key(type, catalog, schema, name)));
        }

        @Override
        public List<FileSpace> list(FileSpaceType type, String catalog, String schema) {
            return List.copyOf(byName.values());
        }

        @Override
        public boolean drop(FileSpaceType type, String catalog, String schema, String name,
                boolean ifExists) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class FakeProfiles implements StorageProfileRegistry {
        private final Map<String, StorageProfile> byName = new HashMap<>();

        void put(StorageProfile profile) {
            byName.put(profile.name(), profile);
        }

        @Override
        public void register(StorageProfile profile) {
            byName.put(profile.name(), profile);
        }

        @Override
        public boolean createIfAbsent(StorageProfile profile) {
            return byName.putIfAbsent(profile.name(), profile) == null;
        }

        @Override
        public boolean unregister(String name) {
            return byName.remove(name) != null;
        }

        @Override
        public Optional<StorageProfile> get(String name) {
            return Optional.ofNullable(byName.get(name));
        }

        @Override
        public Optional<StorageProfileRevision> getCurrentRevision(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<StorageProfileRevision> findRevision(String name, String incarnationId,
                long revision) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<StorageProfile> list() {
            return List.copyOf(byName.values());
        }

        @Override
        public Optional<StorageProfile> resolveByUrl(String url) {
            return byName.values().stream().filter(profile -> profile.matchesUrl(url)).findFirst();
        }
    }
}
