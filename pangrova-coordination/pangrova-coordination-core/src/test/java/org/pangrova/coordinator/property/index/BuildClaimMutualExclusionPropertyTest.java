package org.pangrova.coordinator.property.index;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.pangrova.coordinator.indexing.lifecycle.MetadataBackedBuildClaimHandler;
import org.pangrova.indexing.build.BuildClaimHandler;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.indexing.store.IndexFileMapping;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property P1: Build Claim Mutual Exclusion.
 *
 * <p>For any indexId and any number of concurrent claim attempts,
 * at most one tryClaim returns true. After release, a new tryClaim succeeds.</p>
 *
 * <p>Uses a real {@link MetadataBackedBuildClaimHandler} backed by an
 * in-memory {@link IndexMetadataStore} with CAS-based claim semantics.</p>
 *
 * <p><b>Validates: Requirement 1.5 (Design Property 1)</b></p>
 */
class BuildClaimMutualExclusionPropertyTest {

    @Property(tries = 20)
    void atMostOneClaimSucceedsUnderConcurrency(
            @ForAll("indexIds") String indexId,
            @ForAll("generations") int generation,
            @ForAll("callerCounts") int callerCount) throws Exception {

        ClaimOnlyMetadataStore store = new ClaimOnlyMetadataStore();
        BuildClaimHandler handler = new MetadataBackedBuildClaimHandler(store);
        ExecutorService executor = Executors.newFixedThreadPool(callerCount);
        try {
            CountDownLatch ready = new CountDownLatch(callerCount);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < callerCount; i++) {
                String ownerId = "node-" + i;
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    go.await();
                    return handler.tryClaim(indexId, generation, ownerId, Instant.now());
                }));
            }
            ready.await();
            go.countDown();
            int claimCount = 0;
            for (Future<Boolean> f : futures) {
                if (f.get()) { claimCount++; }
            }
            assertThat(claimCount)
                    .as("At most one claim among %d callers for %s:%d", callerCount, indexId, generation)
                    .isLessThanOrEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Property(tries = 20)
    void claimSucceedsAfterRelease(
            @ForAll("indexIds") String indexId,
            @ForAll("generations") int generation) {

        ClaimOnlyMetadataStore store = new ClaimOnlyMetadataStore();
        BuildClaimHandler handler = new MetadataBackedBuildClaimHandler(store);

        assertThat(handler.tryClaim(indexId, generation, "owner-A", Instant.now()))
                .as("First claim must succeed").isTrue();
        assertThat(handler.tryClaim(indexId, generation, "owner-B", Instant.now()))
                .as("Concurrent claim must fail while held").isFalse();

        handler.release(indexId, generation);

        assertThat(handler.tryClaim(indexId, generation, "owner-B", Instant.now()))
                .as("Claim must succeed after release").isTrue();
    }

    @Property(tries = 20)
    void duplicateClaimBySameOwnerFails(
            @ForAll("indexIds") String indexId,
            @ForAll("generations") int generation) {

        ClaimOnlyMetadataStore store = new ClaimOnlyMetadataStore();
        BuildClaimHandler handler = new MetadataBackedBuildClaimHandler(store);

        assertThat(handler.tryClaim(indexId, generation, "owner-A", Instant.now())).isTrue();
        assertThat(handler.tryClaim(indexId, generation, "owner-A", Instant.now()))
                .as("Duplicate claim by same owner must fail (putIfAbsent semantics)")
                .isFalse();
    }

    @Provide
    Arbitrary<String> indexIds() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20);
    }

    @Provide
    Arbitrary<Integer> generations() {
        return Arbitraries.integers().between(0, 100);
    }

    @Provide
    Arbitrary<Integer> callerCounts() {
        return Arbitraries.integers().between(2, 5);
    }

    /**
     * Minimal {@link IndexMetadataStore} that only implements the build claim
     * methods with real CAS semantics. All other interface methods are
     * intentional no-ops: this test exclusively exercises the claim/release
     * mutual exclusion contract through {@link MetadataBackedBuildClaimHandler},
     * which only delegates to claimBuild/releaseBuildClaim/heartbeatBuildClaim.
     */
    static final class ClaimOnlyMetadataStore implements IndexMetadataStore {

        private final ConcurrentHashMap<String, String> claims = new ConcurrentHashMap<>();

        @Override
        public boolean claimBuild(String indexId, int generation, String ownerId, Instant now) {
            return claims.putIfAbsent(indexId + ":" + generation, ownerId) == null;
        }

        @Override
        public void releaseBuildClaim(String indexId, int generation) {
            claims.remove(indexId + ":" + generation);
        }

        @Override
        public void heartbeatBuildClaim(String indexId, int generation, Instant now) {
            // Heartbeat is a liveness signal; no state mutation needed for exclusion tests
        }

        // ── Non-claim methods: never called by BuildClaimHandler ────────
        //    Intentional no-ops per IndexMetadataStore contract defaults.

        @Override public void registerIndex(IndexDescriptor d) { }
        @Override public void updateIndexState(String id, String s, Instant t) { }
        @Override public boolean beginIndexGeneration(String id, int next, String snapshot,
                Instant timestamp, Instant expected, IndexState state, int generation) { return false; }
        @Override public org.pangrova.indexing.store.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.indexing.store.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<org.pangrova.indexing.store.IndexGenerationManifest>
                findActiveIndexGeneration(String id) { return Optional.empty(); }
        @Override public Optional<IndexDescriptor> findIndex(String id) { return Optional.empty(); }
        @Override public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) { return List.of(); }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(String tenantId, String c, String s, String t, List<String> cols, boolean stale) { return List.of(); }
        @Override public void dropIndex(String id, Instant t) { }
        @Override public void purgeIndex(String id) { }
        @Override public void registerIndexFile(IndexFileMapping m) { }
        @Override public void batchRegisterIndexFiles(List<IndexFileMapping> m) { }
        @Override public List<IndexFileMapping> getIndexFileMappings(String id) { return List.of(); }
        @Override public Map<String, Map<String, String>> batchGetIndexFileMappings(List<String> ids, List<String> paths) { return Map.of(); }
        @Override public void removeIndexFileMapping(String id, String path) { }
        @Override public List<String> filterUnindexedFiles(String id, List<String> paths) { return List.of(); }
        @Override public void markOrphanedByDataFiles(String id, List<String> paths) { }
        @Override public List<IndexFileMapping> listStaleFileMappings(long older) { return List.of(); }
        @Override public void updateLastVerifiedAt(String id, String path, long ts) { }
        @Override public List<IndexDescriptor> findIndexesByColumn(String tenantId, String c, String s, String t, String col) { return List.of(); }
        @Override public void renameColumnInIndexes(String tenantId, String c, String s, String t, String o, String n) { }
        @Override public void dropAllIndexesForTable(String tenantId, String c, String s, String t, Instant ts) { }
        @Override public void renameTable(String tenantId, String c, String s, String o, String n) { }
        @Override public List<IndexDescriptor> findIndexesByState(IndexState state) { return List.of(); }
        @Override public void updateLastIndexedSnapshotId(String id, String snap) { }
    }
}
