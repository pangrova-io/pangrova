package org.pangrova.coordinator.file;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter;
import org.pangrova.security.audit.AuditEvent;
import org.pangrova.security.audit.AuditEventEmitter;
import org.pangrova.security.audit.AuditEventType;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.spi.security.FileSpaceAccessControl;
import org.pangrova.spi.security.UserIdentity;
import org.pangrova.spi.storage.StorageProfile;
import org.pangrova.spi.storage.StorageType;
import org.pangrova.spi.storage.file.FileSpace;
import org.pangrova.spi.storage.file.FileSpaceDefinition;
import org.pangrova.spi.storage.file.FileSpaceMutation;
import org.pangrova.spi.storage.file.FileSpaceRegistry;
import org.pangrova.spi.storage.file.FileSpaceType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileUrlServiceTest {

    private static final String SECRET = "file-url-test-secret";
    private static final long NOW = 1_000_000L;
    private static final UserIdentity USER =
            new UserIdentity("alice", Set.of("analysts"), Map.of("tenant_id", "tenant-a"), Instant.EPOCH);

    private final FakeSpaces spaces = new FakeSpaces();
    private final org.pangrova.coordinator.storage.InMemoryStorageProfileRegistry profiles =
            new org.pangrova.coordinator.storage.InMemoryStorageProfileRegistry();
    private final RecordingAccessControl access = new RecordingAccessControl();
    private final RecordingUrlClient client = new RecordingUrlClient();
    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();
    private final List<AuditEventEmitter> emitters = new java.util.ArrayList<>();

    @AfterEach
    void closeAuditEmitters() {
        for (var emitter : emitters) {
            emitter.close();
            assertThat(emitter.shutdownTimedOut()).isFalse();
            assertThat(emitter.eventsLostDuringShutdown()).isZero();
        }
        emitters.clear();
    }

    @Test
    void issuesAReadUrlAndRecordsTheIssuanceWithoutTheUrl() {
        FileUrlService.IssuedUrl issued = service().issue(USER, "volumes", "media", "raw",
                "landing", "images/a.jpg", true, 600);

        assertThat(issued.url()).isEqualTo("https://storage.example/a.jpg?sig=x");
        assertThat(issued.expiresAtEpochMillis()).isEqualTo(NOW + 5_000);
        assertThat(issued.path()).isEqualTo("images/a.jpg");
        assertThat(issued.mode()).isEqualTo("presigned-read");
        assertThat(client.grant.operation()).isEqualTo("read");
        assertThat(client.read).isTrue();
        assertThat(client.ttlSeconds).isEqualTo(600);
        assertThat(access.readVolumes).isTrue();

        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !events.isEmpty());
        AuditEvent event = events.get(0);
        assertThat(event.eventType()).isEqualTo(AuditEventType.FILE_URL_ISSUED);
        assertThat(event.principal()).isEqualTo("alice");
        assertThat(event.accessedObjects()).singleElement().satisfies(object -> {
            assertThat(object.objectType()).isEqualTo("VOLUME");
            assertThat(object.accessType()).isEqualTo("URL_ISSUE");
        });
        assertThat(event.properties()).containsEntry("mode", "presigned-read")
                .containsEntry("path", "images/a.jpg")
                .containsEntry("ttl_seconds", "600")
                .doesNotContainKey("url")
                .doesNotContainValue("https://storage.example/a.jpg?sig=x");
    }

    @Test
    void uploadUrlRequestsWriteAccess() {
        FileUrlService.IssuedUrl issued = service().issue(USER, "stages", "media", "raw",
                "landing", "uploads/a.jpg", false, 300);

        assertThat(issued.mode()).isEqualTo("presigned-write");
        assertThat(client.grant.operation()).isEqualTo("write");
        assertThat(client.read).isFalse();
        assertThat(access.usedStages).isTrue();
        assertThat(access.wroteStages).isTrue();
    }

    @Test
    void lifetimeDefaultsToOneHourAndClampsToSevenDays() {
        service().issue(USER, "volumes", "media", "raw", "landing", "a", true, 0);
        assertThat(client.ttlSeconds).isEqualTo(FileUrlService.DEFAULT_TTL_SECONDS);

        service().issue(USER, "volumes", "media", "raw", "landing", "a", true, 30L * 24 * 3600);
        assertThat(client.ttlSeconds).isEqualTo(FileUrlService.MAX_TTL_SECONDS);
    }

    @Test
    void deniedWriteNeverReachesTheWorker() {
        access.denyWrites = true;

        assertThatThrownBy(() -> service().issue(USER, "volumes", "media", "raw", "landing",
                "a.jpg", false, 600))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(client.grant).isNull();
    }

    @Test
    void expiredOrOverlongWorkerReceiptsCannotBecomeIssuedUrlsOrSuccessAudits() {
        for (boolean overlong : new boolean[]{false, true}) {
            client.expired = !overlong;
            client.overlong = overlong;
            assertThatThrownBy(() -> service().issue(USER, "volumes", "media", "raw",
                    "landing", "a.jpg", true, 600)).isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("expiry");
        }
        closeAuditEmitters();
        assertThat(events).isEmpty();
    }

    @Test
    void aWorkerSigningFailureDoesNotRecordASuccessfulIssuance() {
        client.failure = new IllegalStateException("provider unavailable");
        assertThatThrownBy(() -> service().issue(USER, "volumes", "media", "raw",
                "landing", "a.jpg", true, 600)).isSameAs(client.failure);
        closeAuditEmitters();
        assertThat(events).isEmpty();
    }

    private FileUrlService service() {
        spaces.put(new FileSpace(UUID.randomUUID(), FileSpaceType.VOLUME, "media", "raw", "landing", false,
                "media", "", "volumes/media/raw/landing", false, "", "alice", Instant.EPOCH, Instant.EPOCH));
        spaces.put(new FileSpace(UUID.randomUUID(), FileSpaceType.STAGE, "media", "raw", "landing", true,
                "media", "s3://acme/warehouse/stage/landing", "", false, "", "alice", Instant.EPOCH,
                Instant.EPOCH));
        profiles.register(profile());
        var emitter = new AuditEventEmitter(events::add);
        emitters.add(emitter);
        var context = new FileSpaceAccessService.FileSpaceAccessContext("media",
                Duration.ofMinutes(5), () -> NOW, () -> List.of("http://w1:8080"),
                new FileSpaceAuditEmitter(emitter), SECRET,
                FileAuthorityTestFixture.authority());
        var grants = new FileSpaceAccessService(spaces, profiles, access, context);
        return new FileUrlService(grants, client, () -> NOW);
    }

    private static StorageProfile profile() {
        return new StorageProfile("media", StorageType.S3, "acme", "warehouse",
                "http://localhost:9000", "us-east-1", Map.of(),
                Map.of("path-style-access", "true"));
    }

    private static final class RecordingUrlClient implements FileUrlClient {
        FileSpaceAccessService.FileAccessGrant grant;
        boolean read;
        long ttlSeconds;
        boolean expired;
        boolean overlong;
        RuntimeException failure;

        @Override
        public FileUrl presign(FileSpaceAccessService.FileAccessGrant grant, boolean read,
                long ttlSeconds) {
            this.grant = grant;
            this.read = read;
            this.ttlSeconds = ttlSeconds;
            if (failure != null) throw failure;
            long expiry = expired ? NOW : overlong ? grant.expiresAtEpochMillis() + 1 : NOW + 5_000;
            return new FileUrl("https://storage.example/a.jpg?sig=x", expiry);
        }
    }

    private static final class RecordingAccessControl implements FileSpaceAccessControl {
        boolean readVolumes;
        boolean usedStages;
        boolean wroteStages;
        boolean denyWrites;

        @Override
        public void checkCanReadVolume(UserIdentity user, String catalog, String schema,
                String name) {
            readVolumes = true;
        }

        @Override
        public void checkCanWriteVolume(UserIdentity user, String catalog, String schema,
                String name) {
            if (denyWrites) {
                throw new AccessDeniedException("WRITE_VOLUME is required");
            }
        }

        @Override
        public void checkCanUseStage(UserIdentity user, String catalog, String schema,
                String name) {
            usedStages = true;
        }

        @Override
        public void checkCanReadStage(UserIdentity user, String catalog, String schema,
                String name) {
        }

        @Override
        public void checkCanWriteStage(UserIdentity user, String catalog, String schema,
                String name) {
            if (denyWrites) {
                throw new AccessDeniedException("WRITE_STAGE is required");
            }
            wroteStages = true;
        }
    }

    private static final class FakeSpaces implements FileSpaceRegistry {
        private final Map<String, FileSpace> byName = new HashMap<>();

        void put(FileSpace space) {
            byName.put(space.type() + "/" + space.catalog() + "/" + space.schema() + "/"
                    + space.name(), space);
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
            return Optional.ofNullable(byName.get(
                    type + "/" + catalog + "/" + schema + "/" + name));
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


}
