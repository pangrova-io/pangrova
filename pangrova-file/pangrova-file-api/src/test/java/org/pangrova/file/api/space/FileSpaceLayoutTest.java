package org.pangrova.file.api.space;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.storage.StorageType;
import org.pangrova.filesystem.api.object.ObjectLocation;
import org.pangrova.filesystem.api.profile.StorageProfile;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Layout shared by the real control-plane descriptor issuer and Worker object access. */
class FileSpaceLayoutTest {
    @TempDir Path root;

    @Test
    void managedRootStaysInsideProfileAndSurvivesNamespaceRename() {
        var profile = profile(StorageType.LOCAL, root.toString(), "tenant-a");
        UUID id = UUID.randomUUID();
        String managed = "managed/volumes/" + id;
        var original = new FileSpace(id, FileSpaceType.VOLUME, "media", "raw", "landing", false,
                "profile", "", managed, false, "", "alice", Instant.EPOCH, Instant.EPOCH);
        var renamed = new FileSpace(id, original.type(), original.catalog(), original.schema(), "renamed", false,
                original.storageProfile(), "", managed, false, "", "alice", Instant.EPOCH, Instant.EPOCH);
        String key = FileSpaceLayout.managedKey(original, profile);
        assertThat(key).isEqualTo("tenant-a/" + managed);
        assertThat(FileSpaceLayout.managedKey(renamed, profile)).isEqualTo(key);
        assertThat(new ObjectLocation(profile, key).key("images/a.jpg")).isEqualTo(key + "/images/a.jpg");
    }

    @Test
    void externalS3RootMustStayWithinExactBucketAndPrefixBoundary() {
        var profile = profile(StorageType.S3, "acme", "landing");
        assertThat(FileSpaceLayout.externalBaseKey(external("s3://acme/landing/stage"), profile))
                .isEqualTo("landing/stage");
        for (String location : List.of("s3://other/landing", "s3://acme/landing-sibling", "s3://acme/other")) {
            assertThatThrownBy(() -> FileSpaceLayout.externalBaseKey(external(location), profile))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED));
        }
    }

    @Test
    void localExternalUriIsRelativeToBucketAndConfinedToProfilePrefix() {
        var profile = profile(StorageType.LOCAL, root.toString(), "tenant-a");
        assertThat(FileSpaceLayout.externalBaseKey(
                external(root.resolve("tenant-a/stage space").toUri().toString()), profile))
                .isEqualTo("tenant-a/stage space");
        for (String location : List.of(root.resolve("tenant-ab/stage").toUri().toString(),
                root.resolve("tenant-a/../secret").toUri().toString(), "s3://bucket/stage", "file://remote-host/stage")) {
            assertThatThrownBy(() -> FileSpaceLayout.externalBaseKey(external(location), profile))
                    .isInstanceOfSatisfying(PangrovaException.class, failure ->
                            assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_STORAGE_NOT_CONFIGURED));
        }
    }

    private static FileSpace external(String location) {
        return new FileSpace(UUID.randomUUID(), FileSpaceType.STAGE, "media", "raw", "landing", true,
                "profile", location, "", true, "", "alice", Instant.EPOCH, Instant.EPOCH);
    }

    private static StorageProfile profile(StorageType type, String bucket, String prefix) {
        return new StorageProfile("profile", type, bucket, prefix, "", "us-east-1", Map.of(), Map.of());
    }
}
