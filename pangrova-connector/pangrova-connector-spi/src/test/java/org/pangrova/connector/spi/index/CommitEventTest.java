package org.pangrova.connector.spi.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CommitEventTest {
    @Test
    void fileListsAreCanonicalAndDefensivelyCopied() {
        var added = new ArrayList<>(List.of("s3://bucket/a.parquet"));
        var removed = new ArrayList<>(List.of("s3://bucket/b.parquet"));
        var event = event(added, removed, null);
        added.clear();
        removed.clear();
        assertThat(event.addedFiles()).containsExactly("s3://bucket/a.parquet");
        assertThat(event.removedFiles()).containsExactly("s3://bucket/b.parquet");
        assertThatThrownBy(() -> event.addedFiles().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> event.removedFiles().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(event(null, null, null).addedFiles()).isEmpty();
        assertThat(event(null, null, null).removedFiles()).isEmpty();
    }

    @Test
    void nullFileEntriesAndBlankPathsKeepTheirOriginalFailures() {
        for (String path : Arrays.asList(null, "", " ")) {
            Class<? extends Throwable> failure = path == null ? NullPointerException.class : IllegalArgumentException.class;
            assertThatThrownBy(() -> event(Arrays.asList(path), List.of(), null)).isInstanceOf(failure);
            assertThatThrownBy(() -> event(List.of(), Arrays.asList(path), null)).isInstanceOf(failure);
        }
    }

    @Test
    void bothListsAreCopiedBeforeBlankPathValidationForMultipleInvalidFields() {
        assertThatThrownBy(() -> event(List.of(" "), Arrays.asList((String) null), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void ordinaryEventsHaveExplicitAbsentFingerprintAndJournalEventsRequireExactLowercaseDigest() {
        assertThat(event(List.of(), List.of(), null).commitFingerprint()).isNull();
        String digest = "a1".repeat(32);
        assertThat(event(List.of(), List.of(), digest).commitFingerprint()).isEqualTo(digest);
        for (String invalid : List.of("", " ", "a".repeat(63), "a".repeat(65), "A".repeat(64), "g".repeat(64))) {
            assertThatThrownBy(() -> event(List.of(), List.of(), invalid))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("lowercase SHA-256");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " "})
    void everyAuthorityAndCommitIdentityMustBeNonblank(String invalid) {
        rejectIdentity(invalid);
    }

    @Test
    void nullAuthorityAndCommitIdentityAreNotOrdinaryEventDefaults() {
        rejectIdentity(null);
        assertThatThrownBy(() -> new CommitEvent("tenant", "catalog", "schema", "table", List.of(), List.of(),
                null, "snapshot", CommitEventType.APPEND, null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("commitTimestamp");
        assertThatThrownBy(() -> new CommitEvent("tenant", "catalog", "schema", "table", List.of(), List.of(),
                Instant.EPOCH, "snapshot", null, null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("eventType");
    }

    private static void rejectIdentity(String invalid) {
        for (int index = 0; index < 5; index++) {
            var values = new ArrayList<>(List.of("tenant", "catalog", "schema", "table", "snapshot"));
            values.set(index, invalid);
            assertThatThrownBy(() -> new CommitEvent(values.get(0), values.get(1), values.get(2), values.get(3),
                    List.of(), List.of(), Instant.EPOCH, values.get(4), CommitEventType.APPEND, null))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("must not be blank");
        }
    }

    private static CommitEvent event(List<String> added, List<String> removed, String fingerprint) {
        return new CommitEvent("tenant", "catalog", "schema", "table", added, removed,
                Instant.EPOCH, "snapshot", CommitEventType.APPEND, fingerprint);
    }
}
