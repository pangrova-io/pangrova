package org.pangrova.connector.lance;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.concurrent.ManagedExecutorFactory;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.store.jdbc.catalog.JdbcMetadataStore;
import org.pangrova.protocol.v2.AcceptedSourceSnapshotRef;
import org.pangrova.protocol.v2.LanceWriteOperation;
import org.pangrova.connector.spi.coordination.WriteCommitEntry;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** One real production PG authority and the matching fixed SDK, without a simulated Native control port. */
class ManagedLancePublicationIntegrationTest extends ContainerTestBase {
    private static final Schema SCHEMA = new Schema(List.of(Field.nullable("id", new ArrowType.Int(64, true))));
    @TempDir Path directory;

    @Test
    void fixtureWarehouseComesFromCurrentPgDeploymentAndRejectsStaleOrRevokedBindings() {
        String name = "warehouse_" + UUID.randomUUID();
        var fixture = new LanceManagedSourceFixture(name, directory);
        var unusedCallerRoot = directory.resolve("unused-caller-root");
        var sameDeployment = new LanceManagedSourceFixture(name, unusedCallerRoot);
        assertThat(fixture.warehouse()).isEqualTo(directory.toAbsolutePath().normalize());
        assertThat(sameDeployment.warehouse()).isEqualTo(fixture.warehouse());
        assertThat(Files.exists(unusedCallerRoot)).isFalse();
        var replacement = directory.resolve("replacement").toAbsolutePath().normalize();
        revise(fixture, Map.of("warehouse", replacement.toString()), Set.of(LanceManagedSourceFixture.TENANT));
        assertThatThrownBy(fixture::warehouse).isInstanceOf(SecurityException.class);
        assertThatThrownBy(sameDeployment::warehouse).isInstanceOf(SecurityException.class);
        var current = new LanceManagedSourceFixture(name, unusedCallerRoot);
        assertThat(current.warehouse()).isEqualTo(replacement);
        var metadata = new JdbcMetadataStore(current.dataSource());
        assertThat(metadata.getCatalogDescriptor(name).orElseThrow().properties().get("warehouse"))
                .isEqualTo(current.warehouse().toString());
        revise(current, Map.of("warehouse", replacement.toString()), Set.of("another-tenant"));
        assertThatThrownBy(current::warehouse).isInstanceOf(SecurityException.class);
        var revoked = new LanceManagedSourceFixture(name, unusedCallerRoot);
        assertThatThrownBy(revoked::warehouse).isInstanceOf(SecurityException.class);
    }

    @Test
    void preparationRecordsTheListeningSdkActorAndRejectsMissingEndpointsBeforeIssuance() throws Exception {
        var fixture = new LanceManagedSourceFixture("endpoint_" + UUID.randomUUID(), directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("endpoint.lance").toString();
        var input = stream("endpoint", 71, 72);
        var allocation = sdk.allocate(input, uri, "endpoint", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE,
                0, 0, SCHEMA);
        var fragment = sdk.write(input, allocation);
        var request = fixture.reserve(allocation.executionId(), List.of(fragment));
        assertThatThrownBy(() -> fixture.issuePreparation(request, directory.resolve("absent.sock"),
                "unreachable-negative-endpoint-regression"))
                .isInstanceOf(java.io.IOException.class);
        try (var connection = fixture.dataSource().getConnection();
             var statement = connection.prepareStatement(
                     "SELECT count(*) FROM managed_source_prepare_invocations WHERE execution_id=?")) {
            statement.setString(1, allocation.executionId());
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getLong(1)).isZero();
            }
        }
        var pages = sdk.prepare(request);
        var accepted = sdk.accept(allocation, pages);
        assertThat(pages).hasSize(1);
        try (var connection = fixture.dataSource().getConnection();
             var statement = connection.prepareStatement(
                     "SELECT worker_endpoint,state,request_sha256,response_sha256 "
                             + "FROM managed_source_prepare_invocations WHERE invocation_id=?")) {
            statement.setString(1, pages.get(0).getInvocationId());
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                var endpoint = java.net.URI.create(result.getString("worker_endpoint"));
                assertThat(endpoint.getScheme()).isEqualTo("unix");
                assertThat(endpoint.getPath()).startsWith("/tmp/lance-sdk-").endsWith("/actor.sock");
                assertThat(Files.exists(Path.of(endpoint.getPath()))).isFalse();
                assertThat(result.getString("state")).isEqualTo("DRAINED");
                assertThat(result.getBytes("request_sha256")).containsExactly(
                        MessageDigest.getInstance("SHA-256").digest(request.toByteArray()));
                assertThat(result.getBytes("response_sha256")).containsExactly(
                        MessageDigest.getInstance("SHA-256").digest(pages.get(0).toByteArray()));
                assertThat(result.next()).isFalse();
            }
        }
        try (var allocator = new RootAllocator()) {
            assertThat(sdk.read(accepted, allocator)).containsExactly(List.of(71L), List.of(72L));
        }
        assertAcceptedObjects(accepted);
    }

    @Test
    void acceptedHeadAndHistoryUseExactPreparedManifestThroughCreateAppendAndOverwrite() throws Exception {
        String name = "sdk_" + UUID.randomUUID();
        var fixture = new LanceManagedSourceFixture(name, directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("history.lance").toString();
        var first = write(fixture, sdk, uri, "create", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 11, 12);
        var second = write(fixture, sdk, uri, "append", LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND, 1, 13);
        var third = write(fixture, sdk, uri, "overwrite", LanceWriteOperation.LANCE_WRITE_OPERATION_OVERWRITE, 2, 99);
        try (var allocator = new RootAllocator()) {
            assertThat(sdk.read(first, allocator)).containsExactly(List.of(11L), List.of(12L));
            assertThat(sdk.read(second, allocator)).containsExactly(List.of(11L), List.of(12L), List.of(13L));
            assertThat(sdk.read(third, allocator)).containsExactly(List.of(99L));
        }
        assertThat(fixture.reference(uri, 0)).isEqualTo(third);
        assertThat(Files.exists(Path.of(uri).resolve("_versions"))).isFalse();
        assertThat(first.getAcceptedOriginCatalogRevision()).isEqualTo(1);
        assertThat(second.getMembershipId()).isNotEqualTo(first.getMembershipId());
        assertAcceptedObjects(first); assertAcceptedObjects(second); assertAcceptedObjects(third);
        try (var connection = fixture.dataSource().getConnection();
             var statement = connection.prepareStatement("SELECT count(*) FROM managed_source_publications WHERE catalog_name=? AND state='ACCEPTED'")) {
            statement.setString(1, name);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue(); assertThat(result.getLong(1)).isEqualTo(3);
            }
        }
    }

    @Test
    void currentDeploymentReauthorizesHistoryWithoutRewritingAcceptedOrigin() throws Exception {
        String name = "revision_" + UUID.randomUUID();
        var original = new LanceManagedSourceFixture(name, directory);
        var originalSdk = new LanceNativeSdkFixture(original, directory);
        String uri = directory.resolve("revision.lance").toString();
        var accepted = write(original, originalSdk, uri, "origin", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 41, 42);
        revise(original, Map.of("warehouse", directory.toString()), Set.of(LanceManagedSourceFixture.TENANT));
        var current = new LanceManagedSourceFixture(name, directory);
        var currentSdk = new LanceNativeSdkFixture(current, directory);
        var historical = current.reference(uri, 1);
        assertThat(historical.getCatalog().getCatalogRevision()).isEqualTo(2);
        assertThat(historical.getAcceptedOriginCatalogRevision()).isEqualTo(1);
        assertThat(historical.toBuilder().setCatalog(accepted.getCatalog()).build()).isEqualTo(accepted);
        try (var allocator = new RootAllocator()) {
            assertThat(currentSdk.read(historical, allocator)).containsExactly(List.of(41L), List.of(42L));
            assertThatThrownBy(() -> currentSdk.read(accepted, allocator)).isInstanceOf(IllegalArgumentException.class);
        }
        try (var connection = current.dataSource().getConnection();
             var statement = connection.prepareStatement("SELECT catalog_revision,manifest_sha256,published_version FROM managed_source_publications WHERE fingerprint=?")) {
            statement.setString(1, accepted.getMembershipId());
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue(); assertThat(result.getLong(1)).isEqualTo(1);
                assertThat(result.getBytes(2)).containsExactly(accepted.getManifestSha256().getDigest().toByteArray());
                assertThat(result.getLong(3)).isEqualTo(1); assertThat(result.next()).isFalse();
            }
        }
        // Replaying the original writer requires its original current deployment, even though a new deployment can read history.
        assertThatThrownBy(() -> original.reference(uri, 1)).isInstanceOf(SecurityException.class);
        revise(current, Map.of("warehouse", directory.toString()), Set.of("another-tenant"));
        var revoked = new LanceManagedSourceFixture(name, directory);
        assertThatThrownBy(() -> revoked.reference(uri, 1)).isInstanceOf(SecurityException.class);
        assertAcceptedObjects(accepted);
    }

    @Test
    void warehouseChangeCannotAuthorizeOldSourceNamespace() throws Exception {
        String name = "namespace_" + UUID.randomUUID();
        var original = new LanceManagedSourceFixture(name, directory);
        String uri = directory.resolve("original.lance").toString();
        var accepted = write(original, new LanceNativeSdkFixture(original, directory), uri,
                "origin", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 51);
        var replacement = Files.createDirectory(directory.resolve("replacement"));
        revise(original, Map.of("warehouse", replacement.toString()), Set.of(LanceManagedSourceFixture.TENANT));
        var current = new LanceManagedSourceFixture(name, replacement);
        assertThatThrownBy(() -> current.reference(uri, 1)).isInstanceOf(SecurityException.class)
                .hasMessageContaining("outside the current catalog namespace");
        assertAcceptedObjects(accepted);
    }

    @Test
    void lostPreparedReplyReconcilesIdenticalSdkCandidateBeforeAnyPgVisibility() throws Exception {
        var fixture = new LanceManagedSourceFixture("unknown_" + UUID.randomUUID(), directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("unknown.lance").toString();
        var input = stream("unknown", 61, 62);
        var allocation = sdk.allocate(input, uri, "unknown", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 0, SCHEMA);
        var fragment = sdk.write(input, allocation);
        var request = fixture.reserve(allocation.executionId(), List.of(fragment));
        // The SDK really finishes a private candidate; the Coordinator has not accepted it or observed the reply.
        var unobserved = sdk.prepare(request);
        var candidate = unobserved.get(0).getPublication().getCandidate();
        byte[] immutable = Files.readAllBytes(Path.of(uri).resolve(candidate.getManifestRelativePath()));
        assertThatThrownBy(() -> fixture.reference(uri, 0)).hasMessageContaining("No accepted source");
        assertThat(fixture.journal().getEntry(allocation.executionId()).orElseThrow().state()).isEqualTo(WriteCommitEntry.State.PENDING);
        var replay = sdk.prepare(request);
        assertThat(withoutInvocation(replay)).isEqualTo(withoutInvocation(unobserved));
        assertThat(replay.get(0).getInvocationId()).isNotEqualTo(unobserved.get(0).getInvocationId());
        assertThat(Files.readAllBytes(Path.of(uri).resolve(candidate.getManifestRelativePath()))).containsExactly(immutable);
        var accepted = sdk.accept(allocation, replay);
        assertThat(accepted).isEqualTo(candidate);
        try (var allocator = new RootAllocator()) {
            assertThat(sdk.read(accepted, allocator)).containsExactly(List.of(61L), List.of(62L));
        }
    }

    @Test
    void abortAfterRealPreparationPreventsAcceptanceAndRetainsUnknownObjects() throws Exception {
        var fixture = new LanceManagedSourceFixture("abort_" + UUID.randomUUID(), directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("aborted.lance").toString();
        var input = stream("abort", 71);
        var allocation = sdk.allocate(input, uri, "abort", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 0, SCHEMA);
        var fragment = sdk.write(input, allocation);
        var pages = sdk.prepare(allocation, List.of(fragment));
        var candidate = pages.get(0).getPublication().getCandidate();
        var receipt = fixture.abort(allocation.executionId());
        assertThat(receipt.executionId()).isEqualTo(allocation.executionId());
        assertThat(receipt.commitFingerprint()).isEqualTo(candidate.getMembershipId());
        assertThat(fixture.journal().getEntry(allocation.executionId()).orElseThrow().state()).isEqualTo(WriteCommitEntry.State.ABORTED);
        assertThatThrownBy(() -> sdk.accept(allocation, pages)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> fixture.reference(uri, 0)).hasMessageContaining("No accepted source");
        assertThat(Files.isRegularFile(Path.of(uri).resolve(candidate.getManifestRelativePath()))).isTrue();
        for (var page : pages) for (var object : page.getObjectPage().getObjectsList()) {
            assertThat(Files.size(Path.of(uri).resolve(object.getRelativePath()))).isEqualTo(object.getSizeBytes());
        }
    }

    @Test
    void twoActualSdkCandidatesFromOneBaseHaveExactlyOneAtomicPgWinner() throws Exception {
        var fixture = new LanceManagedSourceFixture("race_" + UUID.randomUUID(), directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("race.lance").toString();
        var base = write(fixture, sdk, uri, "base", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 81);
        var leftStream = stream("left", 82);
        var rightStream = stream("right", 83);
        var left = sdk.allocate(leftStream, uri, "left", LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND, 1, 0, SCHEMA);
        var right = sdk.allocate(rightStream, uri, "right", LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND, 1, 0, SCHEMA);
        var leftPages = sdk.prepare(left, List.of(sdk.write(leftStream, left)));
        var rightPages = sdk.prepare(right, List.of(sdk.write(rightStream, right)));
        var leftPrepared = fixture.registerObjects(left.executionId(), leftPages.get(0));
        var rightPrepared = fixture.registerObjects(right.executionId(), rightPages.get(0));
        assertThat(leftPages).hasSize(1); assertThat(rightPages).hasSize(1);
        var ready = new CountDownLatch(2);
        var start = new CountDownLatch(1);
        var executor = ManagedExecutorFactory.newBoundedThreadPool("actual-pg-source-cas", 2, 2);
        RaceResult leftResult;
        RaceResult rightResult;
        try {
            var leftFuture = executor.submit(() -> acceptRace(fixture, left.executionId(), leftPrepared, ready, start));
            var rightFuture = executor.submit(() -> acceptRace(fixture, right.executionId(), rightPrepared, ready, start));
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
            leftResult = leftFuture.get(30, TimeUnit.SECONDS); rightResult = rightFuture.get(30, TimeUnit.SECONDS);
        } finally {
            start.countDown(); executor.shutdownNow(); assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(List.of(leftResult, rightResult).stream().filter(result -> result.reference() != null).count()).isEqualTo(1);
        var winner = leftResult.reference() != null ? leftResult : rightResult;
        var loser = leftResult.reference() == null ? leftResult : rightResult;
        assertThat(loser.failure()).isInstanceOf(IllegalStateException.class);
        assertThat(fixture.reference(uri, 0)).isEqualTo(winner.reference());
        assertThat(fixture.journal().getEntry(winner.execution()).orElseThrow().state()).isEqualTo(WriteCommitEntry.State.COMMITTED);
        assertThat(fixture.journal().getEntry(loser.execution()).orElseThrow().state()).isEqualTo(WriteCommitEntry.State.PENDING);
        var aborted = fixture.abort(loser.execution());
        assertThat(aborted.executionId()).isEqualTo(loser.execution());
        assertThat(fixture.journal().getEntry(loser.execution()).orElseThrow().state()).isEqualTo(WriteCommitEntry.State.ABORTED);
        try (var allocator = new RootAllocator()) {
            assertThat(sdk.read(base, allocator)).containsExactly(List.of(81L));
            assertThat(sdk.read(winner.reference(), allocator)).containsExactly(List.of(81L),
                    List.of(winner.execution().equals(left.executionId()) ? 82L : 83L));
        }
        assertAcceptedObjects(leftPages.get(0).getPublication().getCandidate());
        assertAcceptedObjects(rightPages.get(0).getPublication().getCandidate());
    }

    @Test
    void unissuedObjectPageAndConflictingReplayRollBackBeforeVisibility() throws Exception {
        var fixture = new LanceManagedSourceFixture("pages_" + UUID.randomUUID(), directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("pages.lance").toString();
        var input = stream("pages", 91);
        var allocation = sdk.allocate(input, uri, "pages", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 0, SCHEMA);
        var pages = sdk.prepare(allocation, List.of(sdk.write(input, allocation)));
        assertThat(pages).hasSize(1);
        var first = pages.get(0);
        var unissuedPage = first.toBuilder().setObjectPage(first.getObjectPage().toBuilder()
                .setPageIndex(1).setAfterPath("!")).build();
        assertThatThrownBy(() -> fixture.registerObjects(allocation.executionId(), unissuedPage))
                .hasMessageContaining("sequence or cursor differs");
        try (var connection = fixture.dataSource().getConnection();
             var statement = connection.prepareStatement("SELECT registered_count,manifest_sha256 FROM managed_source_publications WHERE fingerprint=?")) {
            statement.setString(1, first.getCommitFingerprint());
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue(); assertThat(result.getLong(1)).isZero(); assertThat(result.getBytes(2)).isNull();
            }
        }
        var prepared = fixture.registerObjects(allocation.executionId(), first);
        fixture.registerObjects(allocation.executionId(), first); // Same real receipt is idempotent.
        var lastFlagConflict = first.toBuilder().setObjectPage(first.getObjectPage().toBuilder().setIsLast(false)).build();
        assertThatThrownBy(() -> fixture.registerObjects(allocation.executionId(), lastFlagConflict))
                .hasMessageContaining("replay conflicts");
        assertThatThrownBy(() -> fixture.reference(uri, 0)).hasMessageContaining("No accepted source");
        var accepted = fixture.acceptRegistered(allocation.executionId(), prepared);
        assertThat(accepted).isEqualTo(first.getPublication().getCandidate());
        try (var allocator = new RootAllocator()) { assertThat(sdk.read(accepted, allocator)).containsExactly(List.of(91L)); }
    }

    @Test
    void sdkAggregateTransactionConditionalCreateRejectsDifferentStoredBytes() throws Exception {
        var fixture = new LanceManagedSourceFixture("immutable_" + UUID.randomUUID(), directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("immutable.lance").toString();
        var input = stream("immutable", 101);
        var allocation = sdk.allocate(input, uri, "immutable", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 0, SCHEMA);
        var request = fixture.reserve(allocation.executionId(), List.of(sdk.write(input, allocation)));
        var pages = sdk.prepare(request);
        var transaction = pages.stream().flatMap(page -> page.getObjectPage().getObjectsList().stream())
                .filter(object -> object.getRelativePath().startsWith("_transactions/")).findFirst().orElseThrow();
        var path = Path.of(uri).resolve(transaction.getRelativePath());
        byte[] original = Files.readAllBytes(path);
        byte[] different = original.clone(); different[0] ^= 1;
        var manifestPath = Path.of(uri).resolve(pages.get(0).getPublication().getCandidate().getManifestRelativePath());
        byte[] manifestBytes = Files.readAllBytes(manifestPath);
        Files.write(path, different);
        // Make the actual candidate absent so this attempt reaches the official SDK's
        // transaction put. An existing candidate would only exercise read validation.
        Files.delete(manifestPath);
        assertThatThrownBy(() -> sdk.prepare(request)).isInstanceOf(IllegalStateException.class);
        assertThat(Files.readAllBytes(path)).containsExactly(different);
        assertThat(Files.exists(manifestPath)).isFalse();
        assertThatThrownBy(() -> fixture.reference(uri, 0)).hasMessageContaining("No accepted source");
        assertThat(fixture.journal().getEntry(allocation.executionId()).orElseThrow().state()).isEqualTo(WriteCommitEntry.State.PENDING);
        // Restore the actual canonical object only in this adversarial fixture; production never overwrites it.
        Files.write(path, original);
        Files.write(manifestPath, manifestBytes);
        var replay = sdk.prepare(request);
        assertThat(withoutInvocation(replay)).isEqualTo(withoutInvocation(pages));
        assertThat(sdk.accept(allocation, replay)).isEqualTo(pages.get(0).getPublication().getCandidate());
    }

    @Test
    void concurrentSdkPreparationOfExactFingerprintUsesOneImmutableTransactionAndCandidate() throws Exception {
        var fixture = new LanceManagedSourceFixture("sdk_race_" + UUID.randomUUID(), directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("sdk-race.lance").toString();
        var input = stream("sdk-race", 111, 112);
        var allocation = sdk.allocate(input, uri, "sdk-race", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 0, SCHEMA);
        var request = fixture.reserve(allocation.executionId(), List.of(sdk.write(input, allocation)));
        var start = new CountDownLatch(1);
        var ready = new CountDownLatch(2);
        var executor = ManagedExecutorFactory.newBoundedThreadPool("actual-sdk-immutable-race", 2, 2);
        SdkRacePreparation leftOutcome; SdkRacePreparation rightOutcome;
        try {
            java.util.concurrent.Callable<SdkRacePreparation> prepare = () -> {
                ready.countDown();
                if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Actual SDK race did not start");
                try { return new SdkRacePreparation(sdk.prepare(request), null); }
                catch (IllegalStateException unknown) { return new SdkRacePreparation(null, unknown); }
            };
            var leftFuture = executor.submit(prepare); var rightFuture = executor.submit(prepare);
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
            leftOutcome = leftFuture.get(60, TimeUnit.SECONDS); rightOutcome = rightFuture.get(60, TimeUnit.SECONDS);
        } finally {
            start.countDown(); executor.shutdownNow(); assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(List.of(leftOutcome, rightOutcome).stream().filter(value -> value.pages() != null).count()).isGreaterThanOrEqualTo(1);
        for (var outcome : List.of(leftOutcome, rightOutcome)) if (outcome.unknown() != null) {
            assertThat(outcome.unknown()).hasMessage("Actual fixed SDK preparation is UNKNOWN");
        }
        // The losing SDK conditional create may leave an async multipart abort owner.
        // Its issuance remains held; a new read-only invocation can reconcile only the immutable candidate.
        var left = leftOutcome.pages() != null ? leftOutcome.pages() : sdk.prepare(request);
        var right = rightOutcome.pages() != null ? rightOutcome.pages() : sdk.prepare(request);
        assertThat(withoutInvocation(left)).isEqualTo(withoutInvocation(right));
        assertThat(left.get(0).getInvocationId()).isNotEqualTo(right.get(0).getInvocationId());
        assertThatThrownBy(() -> fixture.reference(uri, 0)).hasMessageContaining("No accepted source");
        var accepted = sdk.accept(allocation, left);
        assertThat(fixture.accepted(allocation.executionId())).contains(accepted);
        assertAcceptedObjects(accepted);
        try (var allocator = new RootAllocator()) {
            assertThat(sdk.read(accepted, allocator)).containsExactly(List.of(111L), List.of(112L));
        }
    }

    @Test
    void actualNewPgOwnerReconcilesPreparedCandidateWithOriginalWinnerProvenance() throws Exception {
        var fixture = new LanceManagedSourceFixture("handoff_" + UUID.randomUUID(), directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("handoff.lance").toString();
        var input = stream("handoff", 121);
        var allocation = sdk.allocate(input, uri, "handoff", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 0, SCHEMA);
        var fragment = sdk.write(input, allocation);
        var original = fixture.reserve(allocation.executionId(), List.of(fragment));
        var prepared = sdk.prepare(original);
        var next = fixture.handoff(allocation.executionId());
        var recovery = fixture.reserve(allocation.executionId(), List.of(fragment));
        assertThat(recovery.getIntentExecutionOwner()).isEqualTo(original.getExecutionOwner());
        assertThat(recovery.getExecutionOwner().getFencingToken()).isEqualTo(next.fencingToken())
                .isGreaterThan(original.getExecutionOwner().getFencingToken());
        assertThat(recovery.getExecutionOwner().getOwnerIncarnation()).isNotEqualTo(original.getExecutionOwner().getOwnerIncarnation());
        assertThat(recovery.getCommitFingerprint()).isEqualTo(original.getCommitFingerprint());
        assertThat(recovery.getOutputBinding()).isEqualTo(original.getOutputBinding());
        assertThat(recovery.getFragmentsList()).isEqualTo(original.getFragmentsList());
        var recovered = sdk.prepare(recovery);
        assertThat(withoutInvocation(recovered)).isEqualTo(withoutInvocation(prepared));
        assertThat(recovered.get(0).getInvocationId()).isNotEqualTo(prepared.get(0).getInvocationId());
        var accepted = sdk.accept(allocation, recovered);
        assertThat(fixture.journal().getEntry(allocation.executionId()).orElseThrow().executionFencingToken())
                .isEqualTo(original.getIntentExecutionOwner().getFencingToken());
        try (var allocator = new RootAllocator()) { assertThat(sdk.read(accepted, allocator)).containsExactly(List.of(121L)); }
    }

    @Test
    void positiveAppendAllocationRoundtripPreservesActualPgAcceptedBaseAndStrictEncoding() throws Exception {
        var fixture = new LanceManagedSourceFixture("codec_" + UUID.randomUUID(), directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("codec.lance").toString();
        var base = write(fixture, sdk, uri, "codec-base", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 131);
        var input = stream("codec-append", 132);
        var append = sdk.allocate(input, uri, "codec-append", LanceWriteOperation.LANCE_WRITE_OPERATION_APPEND, 1, 0, SCHEMA);
        var codec = new LanceOutputHandleCodec();
        byte[] bytes = codec.serialize(append.output());
        var decoded = (LanceOutputHandle) codec.deserialize(1, bytes);
        assertThat(codec.serialize(decoded)).containsExactly(bytes);
        var actual = org.pangrova.protocol.source.SourceSnapshotBindings.encode(decoded.acceptedBase(), fixture.catalog());
        assertThat(actual).isEqualTo(base);
        assertThat(actual.getAcceptedOriginCatalogRevision()).isEqualTo(1);
        assertThat(actual.getCatalog()).isEqualTo(fixture.catalog());
        assertThat(actual.getVersion()).isEqualTo(1);
        assertThat(actual.getManifestSha256()).isEqualTo(base.getManifestSha256());
        assertThat(actual.getMembershipId()).isEqualTo(base.getMembershipId());
        assertThat(decoded.table().columns()).containsExactlyElementsOf(append.output().table().columns());
        assertThat(decoded.nativeWriteAllocation().orElseThrow().writeId()).isEqualTo("codec-append");
        assertThatThrownBy(() -> codec.deserialize(2, bytes)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.deserialize(1, java.util.Arrays.copyOf(bytes, bytes.length - 1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.deserialize(1, (new String(bytes, java.nio.charset.StandardCharsets.UTF_8) + " {}")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8))).isInstanceOf(IllegalArgumentException.class);
        var accepted = sdk.accept(append, sdk.prepare(append, List.of(sdk.write(input, append))));
        try (var allocator = new RootAllocator()) {
            assertThat(sdk.read(accepted, allocator)).containsExactly(List.of(131L), List.of(132L));
        }
    }

    @Test
    void canonicalRecoveryIdentityAndSdkCandidateAreOrderIndependentAndFenceGeneration() throws Exception {
        var fixture = new LanceManagedSourceFixture("identity_" + UUID.randomUUID(), directory);
        var sdk = new LanceNativeSdkFixture(fixture, directory);
        String uri = directory.resolve("identity.lance").toString();
        var firstInput = stream("identity-first", 141);
        var secondInput = stream("identity-second", 142);
        var first = sdk.allocate(firstInput, uri, "identity", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 0, SCHEMA);
        var second = sdk.allocate(secondInput, uri, "identity", LanceWriteOperation.LANCE_WRITE_OPERATION_CREATE, 0, 1, SCHEMA);
        byte[] firstFragment = sdk.write(firstInput, first);
        byte[] secondFragment = sdk.write(secondInput, second);
        var request = fixture.reserve(first.executionId(), List.of(secondFragment, firstFragment));
        var entry = fixture.journal().getEntry(first.executionId()).orElseThrow();
        var identity = new org.pangrova.connector.spi.coordination.WriteCommitFingerprint.Intent(entry.queryId(), entry.tenantId(),
                entry.catalogName(), entry.tableName(), entry.writeMode(), entry.baseSnapshotId(), entry.writeOutputBinding(),
                entry.principal(), entry.authorizationReceipt(), null);
        assertThat(org.pangrova.connector.spi.coordination.WriteCommitFingerprint.compute(identity, List.of(firstFragment, secondFragment)))
                .isEqualTo(entry.commitFingerprint());
        var pages = sdk.prepare(request);
        var reordered = request.toBuilder().clearFragments().addFragments(request.getFragments(1)).addFragments(request.getFragments(0)).build();
        assertThat(withoutInvocation(sdk.prepare(reordered))).containsExactlyElementsOf(withoutInvocation(pages));
        var nextBinding = request.getOutputBinding().toBuilder().setLance(request.getOutputBinding().getLance().toBuilder()
                .setGeneration("generation-2")).build();
        var nextIdentity = new org.pangrova.connector.spi.coordination.WriteCommitFingerprint.Intent(entry.queryId(), entry.tenantId(),
                entry.catalogName(), entry.tableName(), entry.writeMode(), entry.baseSnapshotId(), nextBinding.toByteArray(),
                entry.principal(), entry.authorizationReceipt(), null);
        String nextFingerprint = org.pangrova.connector.spi.coordination.WriteCommitFingerprint.compute(nextIdentity, entry.fragments());
        assertThat(nextFingerprint).isNotEqualTo(entry.commitFingerprint());
        var invalid = request.toBuilder().setOutputBinding(nextBinding).setCommitFingerprint(nextFingerprint)
                .setReservation(request.getReservation().toBuilder().setManifestRelativePath("_pangrova/prepared/" + nextFingerprint + ".manifest"))
                .build();
        assertThatThrownBy(() -> sdk.prepare(invalid)).isInstanceOf(IllegalStateException.class);
        assertThat(fixture.store().load(fixture.catalog().getCatalogName(), LanceManagedSourceFixture.TENANT,
                first.output().table().table()).version()).isZero();
        assertThat(fixture.journal().getEntry(entry.queryId()).orElseThrow().publication()).isNull();
        var accepted = sdk.accept(first, pages);
        try (var allocator = new RootAllocator()) {
            assertThat(sdk.read(accepted, allocator)).containsExactlyInAnyOrder(List.of(141L), List.of(142L));
        }
    }

    private static RaceResult acceptRace(LanceManagedSourceFixture fixture, String execution,
            org.pangrova.connector.spi.metadata.PreparedSourcePublication prepared,
            CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Actual PG race did not start");
        try { return new RaceResult(execution, fixture.acceptRegistered(execution, prepared), null); }
        catch (RuntimeException failure) { return new RaceResult(execution, null, failure); }
    }
    private record SdkRacePreparation(List<org.pangrova.protocol.v2.PrepareDistributedWriteResult> pages,
            IllegalStateException unknown) {}
    private record RaceResult(String execution, AcceptedSourceSnapshotRef reference, RuntimeException failure) {}

    private AcceptedSourceSnapshotRef write(LanceManagedSourceFixture fixture, LanceNativeSdkFixture sdk,
            String uri, String writeId, LanceWriteOperation operation, long baseVersion, long... values) throws Exception {
        var input = stream(writeId, values);
        var allocation = sdk.allocate(input, uri, writeId, operation, baseVersion, 0, SCHEMA);
        var fragment = sdk.write(input, allocation);
        return sdk.accept(allocation, sdk.prepare(allocation, List.of(fragment)));
    }
    private Path stream(String name, long... values) throws Exception {
        var path = directory.resolve(name + ".arrow");
        try (var allocator = new RootAllocator(); var root = VectorSchemaRoot.create(SCHEMA, allocator);
             var output = Files.newOutputStream(path); var writer = new ArrowStreamWriter(root, null, output)) {
            root.allocateNew();
            var vector = (BigIntVector) root.getVector(0);
            for (int index = 0; index < values.length; index++) vector.setSafe(index, values[index]);
            root.setRowCount(values.length); writer.start(); writer.writeBatch(); writer.end();
        }
        return path;
    }
    private static void revise(LanceManagedSourceFixture fixture, Map<String, String> properties, Set<String> tenants) {
        var metadata = new JdbcMetadataStore(fixture.dataSource());
        var prior = metadata.getCatalogDescriptor(fixture.catalog().getCatalogName()).orElseThrow();
        metadata.saveCatalogDescriptor(new CatalogDescriptor(prior.name(), prior.type(), prior.provider(), prior.state(),
                properties, prior.comment(), tenants, prior.incarnationId(), prior.revision() + 1));
    }
    private static void assertAcceptedObjects(AcceptedSourceSnapshotRef accepted) throws Exception {
        byte[] manifest = Files.readAllBytes(Path.of(accepted.getDatasetUri()).resolve(accepted.getManifestRelativePath()));
        assertThat(manifest.length).isEqualTo(accepted.getSizeBytes());
        assertThat(MessageDigest.getInstance("SHA-256").digest(manifest)).containsExactly(accepted.getManifestSha256().getDigest().toByteArray());
    }
    private static List<org.pangrova.protocol.v2.PrepareDistributedWriteResult> withoutInvocation(
            List<org.pangrova.protocol.v2.PrepareDistributedWriteResult> pages) {
        for (var page : pages) {
            assertThat(page.getInvocationId()).matches("[0-9a-f]{32}");
            assertThat(page.getSdkJoined()).isTrue();
        }
        return pages.stream().map(page -> page.toBuilder().clearInvocationId().build()).toList();
    }

}
