package org.pangrova.indexing.storage;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NativeIndexArtifactLayoutTest {

    private static final String TENANT_HASH = "e9da86d351cf9a7642d8c50195c3f466";
    private static final String LEGACY_URI = "s3://artifacts/v2/tenants/" + TENANT_HASH
            + "/artifacts/index-id/generation-3";
    private static final String LEGACY_JOB = "index-build:tenant-" + TENANT_HASH + ":index-id:generation:3";
    private static final String ATTEMPT = "00000000-0000-0000-0000-000000000001";

    @Test
    void historicalIdentityRemainsByteCompatible() {
        var layout = new NativeIndexArtifactLayout("s3://artifacts///");
        var identity = NativeIndexArtifactLayout.attempt(
                "tenant", descriptor(3), NativeIndexArtifactLayout.LEGACY_ATTEMPT_ID);

        assertThat(layout.stagingUri(identity)).isEqualTo(LEGACY_URI);
        assertThat(identity.jobId()).isEqualTo(LEGACY_JOB);
        assertThat(identity.idempotencyKey("snap-42")).isEqualTo(LEGACY_JOB + ":snapshot:snap-42");
    }

    @Test
    void currentAttemptUsesItsOwnPrefixAndHashedSnapshotKey() {
        var layout = new NativeIndexArtifactLayout("s3://artifacts");
        var identity = NativeIndexArtifactLayout.attempt("tenant", descriptor(3), ATTEMPT);

        assertThat(layout.stagingUri(identity)).isEqualTo("s3://artifacts/v2/tenants/" + TENANT_HASH
                + "/artifacts/index-id/attempt-" + ATTEMPT + "/generation-3");
        assertThat(identity.jobId()).isEqualTo(LEGACY_JOB + ":attempt:" + ATTEMPT);
        assertThat(identity.idempotencyKey("snap-42")).isEqualTo(LEGACY_JOB + ":attempt:" + ATTEMPT
                + ":snapshot:959c48169b868371e186240c6a4518466a517bd97940b94b135c22d7b3103e9b");
        assertThat(layout.stagingUri(identity)).doesNotStartWith(LEGACY_URI + "/");
    }

    @Test
    void attemptsGenerationsAndTenantsAreDistinct() {
        var layout = new NativeIndexArtifactLayout("/indexes");
        var first = NativeIndexArtifactLayout.attempt("tenant", descriptor(3), ATTEMPT);
        var second = NativeIndexArtifactLayout.attempt(
                "tenant", descriptor(3), "00000000-0000-0000-0000-000000000002");
        var nextGeneration = NativeIndexArtifactLayout.attempt("tenant", descriptor(4), ATTEMPT);
        var otherTenant = NativeIndexArtifactLayout.attempt("tenant-b", descriptor(3), ATTEMPT);

        assertThat(List.of(first, second, nextGeneration, otherTenant))
                .extracting(NativeIndexArtifactLayout.AttemptIdentity::jobId).doesNotHaveDuplicates();
        assertThat(List.of(first, second, nextGeneration, otherTenant))
                .extracting(layout::stagingUri).doesNotHaveDuplicates();
    }

    @Test
    void rejectsInvalidRootsAndUnspecifiedAttempt() {
        assertThatThrownBy(() -> new NativeIndexArtifactLayout(" "))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("baseArtifactUri must not be blank");
        assertThatThrownBy(() -> new NativeIndexArtifactLayout("/"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("baseArtifactUri must not be a filesystem root");
        assertThatThrownBy(() -> NativeIndexArtifactLayout.attempt("tenant", descriptor(3), " "))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("Artifact path identity contains unsafe characters");
    }

    private static IndexDescriptor descriptor(int generation) {
        return new IndexDescriptor(
                "index-id", "by_vector", "catalog", "schema", "table",
                List.of("value"), IndexType.VECTOR, Map.of(),
                IndexState.BUILDING, null, generation, "snap-42", null,
                Instant.EPOCH, Instant.EPOCH, "tenant");
    }
}
