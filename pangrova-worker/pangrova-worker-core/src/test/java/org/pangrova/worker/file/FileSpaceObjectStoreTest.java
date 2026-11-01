package org.pangrova.worker.file;

import java.time.Instant;
import java.util.UUID;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.file.api.profile.StorageProfile;
import org.pangrova.file.api.profile.StorageProfileRegistry;
import org.pangrova.file.api.profile.StorageProfileRevision;
import org.pangrova.common.storage.StorageType;
import org.pangrova.file.api.space.FileSpace;
import org.pangrova.common.file.FileSpaceType;
import org.pangrova.file.api.object.ObjectLocation;
import org.pangrova.file.api.object.ObjectStoreAccessor;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSpaceObjectStoreTest {

    @TempDir
    Path root;

    private final Map<String, StorageProfile> profiles = new HashMap<>();

    @Test
    void managedSpaceIsRootedUnderTheProfileNamespace() {
        profiles.put("local", profile("local", StorageType.LOCAL, root.toString(), "tenant-a"));
        var store = new FileSpaceObjectStore(registry(), "local");

        ObjectLocation location = store.locate(space(FileSpaceType.VOLUME, false,
                "", "", false));

        assertThat(location.baseKey()).isEqualTo("tenant-a/volumes/media/raw/landing");
        assertThat(location.key("images/a.jpg"))
                .isEqualTo("tenant-a/volumes/media/raw/landing/images/a.jpg");
        assertThat(store.accessor(space(FileSpaceType.VOLUME, false, "", "", false)))
                .isInstanceOf(LocalObjectStoreAccessor.class);
    }

    @Test
    void externalSpaceMustStayInsideItsProfile() {
        profiles.put("s3", profile("s3", StorageType.S3, "acme", "landing"));
        var store = new FileSpaceObjectStore(registry(), "s3");

        ObjectLocation location = store.locate(space(FileSpaceType.STAGE, true,
                "s3", "s3://acme/landing/stage", true));
        assertThat(location.baseKey()).isEqualTo("landing/stage");
        assertThat(store.accessor(location.profile())).isInstanceOf(S3ObjectStoreAccessor.class);

        FileSpace outside = space(FileSpaceType.STAGE, true, "s3", "s3://other/landing", true);
        assertThatThrownBy(() -> store.locate(outside))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED));
    }

    @Test
    void localExternalUriIsRelativeToBucketAndConfinedToProfilePrefix() {
        profiles.put("local", profile("local", StorageType.LOCAL, root.toString(), "tenant-a"));
        var store = new FileSpaceObjectStore(registry(), "local");
        var inside = space(FileSpaceType.STAGE, true, "local",
                root.resolve("tenant-a/stage space").toUri().toString(), false);
        assertThat(store.locate(inside).baseKey()).isEqualTo("tenant-a/stage space");
        for (String location : List.of(root.resolve("tenant-ab/stage").toUri().toString(),
                root.resolve("tenant-a/../secret").toUri().toString(), "s3://bucket/stage",
                "file://remote-host/stage")) {
            assertThatThrownBy(() -> store.locate(space(FileSpaceType.STAGE, true,
                    "local", location, false)))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode())
                                    .isEqualTo(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED));
        }
    }

    @Test
    void missingProfileFailsClosedInsteadOfUsingDefaults() {
        var store = new FileSpaceObjectStore(registry(), "");

        assertThatThrownBy(() -> store.locate(space(FileSpaceType.VOLUME, false, "", "", false)))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode())
                                .isEqualTo(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED));
    }

    @Test
    void declaredSpaceProfileOverridesTheManagedDefault() {
        profiles.put("default", profile("default", StorageType.LOCAL,
                root.resolve("default").toString(), ""));
        profiles.put("team", profile("team", StorageType.LOCAL,
                root.resolve("team").toString(), ""));
        var store = new FileSpaceObjectStore(registry(), "default");

        ObjectLocation location = store.locate(space(FileSpaceType.STAGE, false,
                "team", "", true));

        assertThat(location.profile().name()).isEqualTo("team");
        assertThat(location.baseKey()).isEqualTo("stages/media/raw/landing");
    }

    private StorageProfileRegistry registry() {
        return new StorageProfileRegistry() {
            @Override
            public void register(StorageProfile profile) {
                profiles.put(profile.name(), profile);
            }

            @Override
            public boolean createIfAbsent(StorageProfile profile) {
                return profiles.putIfAbsent(profile.name(), profile) == null;
            }

            @Override
            public boolean unregister(String name) {
                return profiles.remove(name) != null;
            }

            @Override
            public Optional<StorageProfile> get(String name) {
                return Optional.ofNullable(profiles.get(name));
            }

            @Override
            public Optional<StorageProfileRevision> getCurrentRevision(String name) {
                return get(name).map(profile -> new StorageProfileRevision(profile,
                        "11111111-1111-1111-1111-111111111111", 1));
            }

            @Override
            public Optional<StorageProfileRevision> findRevision(String name,
                    String incarnationId, long revision) {
                return getCurrentRevision(name);
            }

            @Override
            public List<StorageProfile> list() {
                return List.copyOf(profiles.values());
            }

            @Override
            public Optional<StorageProfile> resolveByUrl(String url) {
                return profiles.values().stream()
                        .filter(profile -> profile.matchesUrl(url)).findFirst();
            }
        };
    }

    private static StorageProfile profile(String name, StorageType type, String bucket,
            String prefix) {
        return new StorageProfile(name, type, bucket, prefix, "", "us-east-1", Map.of(), Map.of());
    }

    private static FileSpace space(FileSpaceType type, boolean external, String profile,
            String location, boolean directory) {
        Instant now = Instant.parse("2026-10-25T00:00:00Z");
        return new FileSpace(UUID.randomUUID(), type, "media", "raw", "landing", external, profile, location,
                external ? "" : type.name().toLowerCase(java.util.Locale.ROOT) + "s/media/raw/landing",
                directory, "", "alice", now, now);
    }
}
