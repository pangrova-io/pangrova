package org.pangrova.connector.lance;

import org.pangrova.persistence.query.api.enums.WriteCommitState;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.protocol.v2.SourceObjectPurpose;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.testing.testkit.ContainerTestBase;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual PG issuance/drain/deletion receipts and official SDK object operations, never inferred from task status. */
class ManagedLanceCleanupIntegrationTest extends ContainerTestBase {
    @TempDir Path directory;

    @Test
    void committedCleanupDeletesOnlyJoinedWinnerSidecarsAndPreservesSdkHistory() throws Exception {
        var fixture = fixture();
        var first = write(fixture, "create", 0, 7, 9); var accepted = first.accept();
        assertInvocationState(fixture, accepted.getMembershipId(), "DRAINED", 1);
        first.sdk().cleanup(first.allocation().executionId());
        assertRoles(first, true);
        assertDeletedCount(fixture, accepted.getMembershipId(), 1);
        first.sdk().cleanup(first.allocation().executionId());
        assertDeletedCount(fixture, accepted.getMembershipId(), 1);
        var appended = write(fixture, "append", 1, 11); var second = appended.accept();
        appended.sdk().cleanup(appended.allocation().executionId());
        assertRoles(appended, true); assertRoles(first, true);
        try (var allocator = new RootAllocator()) {
            assertThat(first.sdk().read(accepted, allocator)).containsExactly(List.of(7), List.of(9));
            assertThat(appended.sdk().read(second, allocator)).containsExactly(List.of(7), List.of(9), List.of(11));
        }
        assertThat(fixture.reference(accepted.getDatasetUri(), 1)).isEqualTo(accepted);
        assertThat(fixture.reference(accepted.getDatasetUri(), 0)).isEqualTo(second);
    }

    @Test
    void sealedAbortedCandidateDeletesAllUnreferencedObjectsOnlyAfterActualJoinReceipts() throws Exception {
        var fixture = fixture(); var prepared = write(fixture, "aborted", 0, 17);
        for (var page : prepared.pages()) fixture.registerObjects(prepared.allocation().executionId(), page);
        var candidate = prepared.pages().get(0).getPublication().getCandidate();
        var receipt = fixture.abort(prepared.allocation().executionId());
        assertThat(receipt.commitFingerprint()).isEqualTo(candidate.getMembershipId());
        assertInvocationState(fixture, candidate.getMembershipId(), "DRAINED", 1);
        prepared.sdk().cleanup(prepared.allocation().executionId());
        for (var page : prepared.pages()) for (var object : page.getObjectPage().getObjectsList()) {
            assertThat(Files.exists(Path.of(candidate.getDatasetUri()).resolve(object.getRelativePath()))).isFalse();
        }
        assertDeletedCount(fixture, candidate.getMembershipId(), candidateCount(prepared));
        prepared.sdk().cleanup(prepared.allocation().executionId());
        assertDeletedCount(fixture, candidate.getMembershipId(), candidateCount(prepared));
        assertThat(fixture.journal().getEntry(prepared.allocation().executionId()).orElseThrow().state()).isEqualTo(WriteCommitState.ABORTED);
        assertThatThrownBy(() -> fixture.reference(candidate.getDatasetUri(), 0)).hasMessageContaining("No accepted source");
        assertThatThrownBy(() -> prepared.accept()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void malformedJoinNonceOrUnjoinedSdkResponseCannotRegisterObjectsOrAuthorizeCleanup() throws Exception {
        var fixture = fixture(); var prepared = write(fixture, "unjoined", 0, 19);
        var page = prepared.pages().get(0); var execution = prepared.allocation().executionId();
        assertThatThrownBy(() -> fixture.registerObjects(execution, page.toBuilder().setSdkJoined(false).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("did not actually join");
        assertThatThrownBy(() -> fixture.registerObjects(execution, page.toBuilder().setInvocationId("0".repeat(32)).build()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no matching durable issuance");
        assertInvocationState(fixture, page.getCommitFingerprint(), "ISSUED", 1);
        assertThat(fixture.cleanupRequest(execution)).isEmpty();
        for (var object : page.getObjectPage().getObjectsList()) {
            assertThat(Files.size(Path.of(page.getPublication().getCandidate().getDatasetUri()).resolve(object.getRelativePath())))
                    .isEqualTo(object.getSizeBytes());
        }
        prepared.accept(); assertInvocationState(fixture, page.getCommitFingerprint(), "DRAINED", 1);
        prepared.sdk().cleanup(execution); assertRoles(prepared, true);
    }

    @Test
    void lostPreparedReplyNeverManufacturesDurableDrainOrDeletion() throws Exception {
        var fixture = fixture(); var prepared = write(fixture, "unknown", 0, 23);
        var candidate = prepared.pages().get(0).getPublication().getCandidate();
        assertInvocationState(fixture, candidate.getMembershipId(), "ISSUED", 1);
        fixture.abort(prepared.allocation().executionId());
        prepared.sdk().cleanup(prepared.allocation().executionId());
        assertInvocationState(fixture, candidate.getMembershipId(), "ISSUED", 1);
        assertDeletedCount(fixture, candidate.getMembershipId(), 0);
        for (var page : prepared.pages()) for (var object : page.getObjectPage().getObjectsList()) {
            assertThat(Files.size(Path.of(candidate.getDatasetUri()).resolve(object.getRelativePath()))).isEqualTo(object.getSizeBytes());
        }
        assertThat(fixture.cleanupRequest(prepared.allocation().executionId())).isEmpty();
    }

    @Test
    void lostDeleteReplyReconcilesSameMarkedBatchAndActualIdempotentSdkDeletion() throws Exception {
        var fixture = fixture(); var prepared = write(fixture, "delete-unknown", 0, 29); var accepted = prepared.accept();
        var request = fixture.cleanupRequest(prepared.allocation().executionId()).orElseThrow();
        var unobserved = prepared.sdk().cleanupPage(request);
        assertThat(unobserved.getSdkJoined()).isTrue();
        assertDeletedCount(fixture, accepted.getMembershipId(), 0);
        var replay = fixture.cleanupRequest(prepared.allocation().executionId()).orElseThrow();
        assertThat(replay.getCleanupId()).isEqualTo(request.getCleanupId());
        assertThat(replay.getObjectsList()).isEqualTo(request.getObjectsList());
        assertThat(replay.getObjectsSha256()).isEqualTo(request.getObjectsSha256());
        var result = prepared.sdk().cleanupPage(replay); assertThat(result).isEqualTo(unobserved);
        assertThatThrownBy(() -> fixture.completeCleanup(prepared.allocation().executionId(), result.toBuilder().setDeletedCount(0).build()))
                .isInstanceOf(IllegalArgumentException.class);
        assertDeletedCount(fixture, accepted.getMembershipId(), 0);
        fixture.completeCleanup(prepared.allocation().executionId(), result);
        fixture.completeCleanup(prepared.allocation().executionId(), result);
        assertDeletedCount(fixture, accepted.getMembershipId(), 1);
        try (var allocator = new RootAllocator()) { assertThat(prepared.sdk().read(accepted, allocator)).containsExactly(List.of(29)); }
    }

    @Test
    void anUnobservedEarlierSdkInvocationKeepsSidecarsHeldAfterLaterAcceptedReplay() throws Exception {
        var fixture = fixture(); var prepared = write(fixture, "held-after-accept", 0, 31);
        var retry = prepared.sdk().prepare(fixture.reserve(prepared.allocation().executionId(),
                fixture.journal().getEntry(prepared.allocation().executionId()).orElseThrow().fragments()));
        var accepted = prepared.sdk().accept(prepared.allocation(), retry);
        assertInvocationState(fixture, accepted.getMembershipId(), "ISSUED", 1);
        assertInvocationState(fixture, accepted.getMembershipId(), "DRAINED", 1);
        prepared.sdk().cleanup(prepared.allocation().executionId()); assertDeletedCount(fixture, accepted.getMembershipId(), 0);
        assertRoles(prepared, false);
        try (var allocator = new RootAllocator()) { assertThat(prepared.sdk().read(accepted, allocator)).containsExactly(List.of(31)); }
    }

    private LanceManagedSourceFixture fixture() { return new LanceManagedSourceFixture("cleanup-" + UUID.randomUUID(), directory); }
    private LancePgSdkTestInputs.Prepared write(LanceManagedSourceFixture fixture, String name, long base, int... values) throws Exception {
        return LancePgSdkTestInputs.write(fixture, directory, directory.resolve("table.lance").toString(), name,
                base == 0 ? LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE : LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND, base, values);
    }
    private static long candidateCount(LancePgSdkTestInputs.Prepared prepared) {
        return prepared.pages().stream().mapToLong(page -> page.getObjectPage().getObjectsCount()).sum();
    }
    private static void assertRoles(LancePgSdkTestInputs.Prepared prepared, boolean deleted) throws Exception {
        int sidecars = 0; int sources = 0;
        for (var page : prepared.pages()) for (var object : page.getObjectPage().getObjectsList()) {
            var path = Path.of(page.getPublication().getCandidate().getDatasetUri()).resolve(object.getRelativePath());
            if (object.getPurpose() == SourceObjectPurpose.SOURCE_OBJECT_PURPOSE_WINNER_SIDECAR) {
                sidecars++; assertThat(Files.exists(path)).isEqualTo(!deleted);
            } else {
                assertThat(object.getPurpose()).isEqualTo(SourceObjectPurpose.SOURCE_OBJECT_PURPOSE_SOURCE);
                sources++; assertThat(Files.size(path)).isEqualTo(object.getSizeBytes());
            }
        }
        assertThat(sidecars).isEqualTo(1); assertThat(sources).isGreaterThanOrEqualTo(3);
    }
    private static void assertInvocationState(LanceManagedSourceFixture fixture, String fingerprint, String state, long expected) throws Exception {
        try (var connection = fixture.dataSource().getConnection(); var statement = connection.prepareStatement(
                "SELECT count(*) FROM managed_source_prepare_invocations WHERE fingerprint=? AND state=?")) {
            statement.setString(1, fingerprint); statement.setString(2, state);
            try (var result = statement.executeQuery()) { result.next(); assertThat(result.getLong(1)).isEqualTo(expected); }
        }
    }
    private static void assertDeletedCount(LanceManagedSourceFixture fixture, String fingerprint, long expected) throws Exception {
        try (var connection = fixture.dataSource().getConnection(); var statement = connection.prepareStatement(
                "SELECT count(*) FROM managed_source_objects WHERE fingerprint=? AND deleted_at IS NOT NULL")) {
            statement.setString(1, fingerprint);
            try (var result = statement.executeQuery()) { result.next(); assertThat(result.getLong(1)).isEqualTo(expected); }
        }
    }
}
