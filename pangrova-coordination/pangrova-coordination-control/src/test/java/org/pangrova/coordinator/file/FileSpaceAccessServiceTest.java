package org.pangrova.coordinator.file;

import org.pangrova.file.api.access.FileAccessGrant;

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
import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.file.FileAccessSignature;
import org.pangrova.common.json.JsonMapperFactory;
import org.pangrova.coordinator.query.audit.FileSpaceAuditEmitter;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.security.api.authorization.FileSpaceAccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.file.api.space.FileSpaceDefinition;
import org.pangrova.file.api.space.FileSpaceMutation;
import org.pangrova.persistence.file.api.space.FileSpaceRegistry;
import org.pangrova.file.api.enums.FileSpaceType;
import org.pangrova.file.access.FileSpaceAccessService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSpaceAccessServiceTest {

    private static final String SECRET = "file-access-test-secret";
    private static final long NOW = 1_000_000L;
    private static final UserIdentity USER =
            new UserIdentity("alice", Set.of("analysts"), Map.of("tenant_id", "tenant-a"), Instant.EPOCH);

    private final ObjectMapper mapper = JsonMapperFactory.create();
    private final FakeSpaces spaces = new FakeSpaces();
    private final org.pangrova.testing.testkit.file.InMemoryStorageProfileRegistry profiles =
            new org.pangrova.testing.testkit.file.InMemoryStorageProfileRegistry();
    private final RecordingAccessControl access = new RecordingAccessControl();

    @Test
    void explicitSpaceProfileOverridesDefaultBeforeDescriptorIssuance() throws Exception {
        profiles.register(profile("media", "default"));
        profiles.register(profile("team", "team-root"));
        spaces.put(space(FileSpaceType.STAGE, false, "team", ""));
        var grant = service(List.of("http://w1:8080"))
                .grant(USER, "stages", "media", "raw", "landing", "file.txt", "read");
        assertThat(descriptor(grant)).containsEntry("profileName", "team")
                .containsEntry("baseKey", "team-root/stages/media/raw/landing");
    }

    @Test
    void managedDefaultIsUsedOnlyWhenConfiguredAndSpaceHasNoExplicitProfile() throws Exception {
        profiles.register(profile("media", "warehouse"));
        spaces.put(space(FileSpaceType.VOLUME, false, "", ""));
        var grant = service(List.of("http://w1:8080"))
                .grant(USER, "volumes", "media", "raw", "landing", "file.txt", "read");
        assertThat(descriptor(grant)).containsEntry("profileName", "media")
                .containsEntry("baseKey", "warehouse/volumes/media/raw/landing");
        assertThatThrownBy(() -> service(List.of("http://w1:8080"), "")
                .grant(USER, "volumes", "media", "raw", "landing", "file.txt", "read"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED));
    }

    @Test
    void externalLocationOutsideProfileIsRejectedBeforeDescriptorIssuance() {
        profiles.register(profile("media", "landing"));
        for (String location : List.of("s3://other/landing", "s3://acme/landing-sibling")) {
            spaces.put(space(FileSpaceType.STAGE, true, "media", location));
            assertThatThrownBy(() -> service(List.of("http://w1:8080"))
                    .grant(USER, "stages", "media", "raw", "landing", "file.txt", "read"))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED));
        }
    }

    @Test
    void managedVolumeReadIssuesSignedDescriptorAtTheManagedRoot() throws Exception {
        spaces.put(space(FileSpaceType.VOLUME, false, "media", "volumes/media/raw/landing"));
        profiles.register(profile("media", "warehouse"));

        FileAccessGrant grant = service(List.of("http://w1:8080"))
                .grant(USER, "volumes", "media", "raw", "landing", "images/a.jpg", "read");

        assertThat(grant.workerUrl()).isEqualTo("http://w1:8080");
        assertThat(grant.path()).isEqualTo("images/a.jpg");
        assertThat(grant.operation()).isEqualTo("read");
        assertThat(grant.expiresAtEpochMillis()).isEqualTo(NOW + Duration.ofMinutes(5).toMillis());
        assertThat(FileAccessSignature.sign(SECRET, grant.descriptor()))
                .isEqualTo(grant.signature());
        assertThat(descriptor(grant)).containsEntry("operation", "read")
                .containsEntry("path", "images/a.jpg")
                .containsEntry("baseKey", "warehouse/volumes/media/raw/landing")
                .containsEntry("bucket", "acme")
                .containsEntry("profileName", "media");
        assertThat(access.readVolumes).isTrue();
        assertThat(access.wroteVolumes).isFalse();
    }

    @Test
    void grantPinsTheNamespaceSubjectTenantAndCurrentProfileIncarnation() throws Exception {
        var namespace = space(FileSpaceType.STAGE, true, "media", "s3://acme/stage/landing");
        spaces.put(namespace);
        profiles.register(profile("media", ""));
        var issuer = service(List.of("http://w1:8080"));
        var original = descriptor(issuer.grant(USER, "stages", "media", "raw", "landing", "x", "write"));
        assertThat(original).containsEntry("spaceId", namespace.id().toString())
                .containsEntry("subject", "alice").containsEntry("tenantId", "tenant-a")
                .containsEntry("profileRevision", 1)
                .containsEntry("profileIncarnationId", profiles.getCurrentRevision("media").orElseThrow().incarnationId());
        profiles.unregister("media");
        profiles.createIfAbsent(profile("media", ""));
        var replaced = descriptor(issuer.grant(USER, "stages", "media", "raw", "landing", "x", "write"));
        assertThat(replaced.get("profileIncarnationId")).isNotEqualTo(original.get("profileIncarnationId"));
        assertThat(replaced.get("baseKey")).isEqualTo(original.get("baseKey"));
    }

    @Test
    void unmappedSubjectsCannotReceiveAFileGrant() {
        spaces.put(space(FileSpaceType.STAGE, false, "media", ""));
        profiles.register(profile("media", ""));
        var tenantless = new UserIdentity("unmapped", Set.of(), Map.of("tenant_id", "tenant-a"), Instant.EPOCH);
        assertThatThrownBy(() -> service(List.of("http://w1:8080"))
                .grant(tenantless, "stages", "media", "raw", "landing", "x", "write"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("tenant");
    }

    @Test
    void externalStageUsesItsDeclaredLocationAsRoot() throws Exception {
        spaces.put(space(FileSpaceType.STAGE, true, "media", "s3://acme/stage/landing"));
        profiles.register(profile("media", ""));

        FileAccessGrant grant = service(List.of("http://w1:8080"))
                .grant(USER, "stages", "media", "raw", "landing", "", "list");

        assertThat(grant.path()).isEmpty();
        assertThat(descriptor(grant)).containsEntry("baseKey", "stage/landing");
        assertThat(access.usedStages).isTrue();
        assertThat(access.readStages).isTrue();
    }

    @Test
    void stageWriteRequiresWritePrivilege() {
        spaces.put(space(FileSpaceType.STAGE, false, "media", ""));
        profiles.register(profile("media", ""));
        access.denyWrites = true;

        assertThatThrownBy(() -> service(List.of("http://w1:8080"))
                .grant(USER, "stages", "media", "raw", "landing", "images/a.jpg", "write"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("WRITE_STAGE");
    }

    @Test
    void volumeWriteAlsoRequiresWritePrivilege() {
        spaces.put(space(FileSpaceType.VOLUME, false, "media", "volumes/media/raw/landing"));
        profiles.register(profile("media", ""));
        access.denyWrites = true;

        assertThatThrownBy(() -> service(List.of("http://w1:8080"))
                .grant(USER, "volumes", "media", "raw", "landing", "a.jpg", "write"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("WRITE_VOLUME");
    }

    @Test
    void traversalIsRejectedBeforeAnyDescriptorExists() {
        spaces.put(space(FileSpaceType.VOLUME, false, "media", "volumes/media/raw/landing"));
        profiles.register(profile("media", ""));

        assertThatThrownBy(() -> service(List.of("http://w1:8080"))
                .grant(USER, "volumes", "media", "raw", "landing", "images/../secret", "read"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_PATH_INVALID));
    }

    @Test
    void missingWorkerFailsClosed() {
        spaces.put(space(FileSpaceType.VOLUME, false, "media", "volumes/media/raw/landing"));
        profiles.register(profile("media", ""));

        assertThatThrownBy(() -> service(List.of())
                .grant(USER, "volumes", "media", "raw", "landing", "a.jpg", "read"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE));
    }

    @Test
    void spaceWithoutProfileFailsClosed() {
        spaces.put(space(FileSpaceType.VOLUME, true, "media", "s3://acme/elsewhere"));

        assertThatThrownBy(() -> service(List.of("http://w1:8080"))
                .grant(USER, "volumes", "media", "raw", "landing", "a.jpg", "read"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED));
    }

    @Test
    void unknownSpaceAndOperationAreDistinguished() {
        profiles.register(profile("media", ""));

        assertThatThrownBy(() -> service(List.of("http://w1:8080"))
                .grant(USER, "volumes", "media", "raw", "missing", "a.jpg", "read"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.METADATA_FILE_SPACE_NOT_FOUND));

        assertThatThrownBy(() -> service(List.of("http://w1:8080"))
                .grant(USER, "volumes", "media", "raw", "landing", "a.jpg", "append"))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.VALIDATION_INVALID_ARGUMENT));
    }

    @Test
    void anonymousCallerIsDenied() {
        assertThatThrownBy(() -> service(List.of("http://w1:8080"))
                .grant(null, "volumes", "media", "raw", "landing", "a.jpg", "read"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void workersAreChosenRoundRobin() {
        spaces.put(space(FileSpaceType.VOLUME, false, "media", "volumes/media/raw/landing"));
        profiles.register(profile("media", ""));
        FileSpaceAccessService service = service(List.of("http://w1:8080", "http://w2:8080"));

        assertThat(service.grant(USER, "volumes", "media", "raw", "landing", "a", "read")
                .workerUrl()).isEqualTo("http://w1:8080");
        assertThat(service.grant(USER, "volumes", "media", "raw", "landing", "b", "read")
                .workerUrl()).isEqualTo("http://w2:8080");
        assertThat(service.grant(USER, "volumes", "media", "raw", "landing", "c", "read")
                .workerUrl()).isEqualTo("http://w1:8080");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> descriptor(FileAccessGrant grant)
            throws Exception {
        byte[] json = Base64.getUrlDecoder().decode(grant.descriptor());
        return mapper.readValue(json, Map.class);
    }

    private FileSpaceAccessService service(List<String> workers) {
        return service(workers, "media");
    }

    private FileSpaceAccessService service(List<String> workers, String managedProfile) {
        var context = new FileSpaceAccessService.FileSpaceAccessContext(managedProfile,
                Duration.ofMinutes(5), () -> NOW, () -> workers,
                new FileSpaceAuditEmitter(null), SECRET,
                org.pangrova.coordinator.file.FileAuthorityTestFixture.authority());
        return new FileSpaceAccessService(spaces, profiles, access, context);
    }

    private static FileSpace space(FileSpaceType type, boolean external, String profile,
            String location) {
        return new FileSpace(UUID.randomUUID(), type, "media", "raw", "landing", external, profile, location,
                external ? "" : type.name().toLowerCase(java.util.Locale.ROOT) + "s/media/raw/landing",
                false, "", "alice", Instant.EPOCH, Instant.EPOCH);
    }

    private static StorageProfile profile(String name, String prefix) {
        return new StorageProfile(name, StorageType.S3, "acme", prefix,
                "http://localhost:9000", "us-east-1", Map.of(),
                Map.of("path-style-access", "true"));
    }

    private static final class RecordingAccessControl implements FileSpaceAccessControl {
        boolean readVolumes;
        boolean wroteVolumes;
        boolean usedStages;
        boolean readStages;
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
            wroteVolumes = true;
        }

        @Override
        public void checkCanUseStage(UserIdentity user, String catalog, String schema,
                String name) {
            usedStages = true;
        }

        @Override
        public void checkCanReadStage(UserIdentity user, String catalog, String schema,
                String name) {
            readStages = true;
        }

        @Override
        public void checkCanWriteStage(UserIdentity user, String catalog, String schema,
                String name) {
            if (denyWrites) {
                throw new AccessDeniedException("WRITE_STAGE is required");
            }
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


}
