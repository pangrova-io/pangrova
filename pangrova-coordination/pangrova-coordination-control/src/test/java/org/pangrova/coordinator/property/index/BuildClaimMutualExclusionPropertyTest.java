package org.pangrova.coordinator.property.index;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository;
import java.util.ArrayList;
import java.util.List;
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
 * <p>Checks the concurrency semantics of a test-only claim implementation.
 * PostgreSQL ownership is verified separately against the real backend.</p>
 *
 * <p><b>Validates: Requirement 1.5 (Design Property 1)</b></p>
 */
class BuildClaimMutualExclusionPropertyTest {

    @Property(tries = 20)
    void atMostOneClaimSucceedsUnderConcurrency(
            @ForAll("indexIds") String indexId,
            @ForAll("generations") int generation,
            @ForAll("callerCounts") int callerCount) throws Exception {

        IndexBuildClaimRepository handler = new ClaimOnlyRepository();
        ExecutorService executor = Executors.newFixedThreadPool(callerCount);
        try {
            CountDownLatch ready = new CountDownLatch(callerCount);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Boolean>> futures = new ArrayList<>();
            for (int i = 0; i < callerCount; i++) {
                String ownerId = "node-" + i;
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    assertThat(go.await(5, TimeUnit.SECONDS)).isTrue();
                    return handler.tryClaim(indexId, generation, ownerId, Instant.now());
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            int claimCount = 0;
            for (Future<Boolean> f : futures) {
                if (f.get(5, TimeUnit.SECONDS)) { claimCount++; }
            }
            assertThat(claimCount)
                    .as("At most one claim among %d callers for %s:%d", callerCount, indexId, generation)
                    .isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Property(tries = 20)
    void claimSucceedsAfterRelease(
            @ForAll("indexIds") String indexId,
            @ForAll("generations") int generation) {

        IndexBuildClaimRepository handler = new ClaimOnlyRepository();

        assertThat(handler.tryClaim(indexId, generation, "owner-A", Instant.now()))
                .as("First claim must succeed").isTrue();
        assertThat(handler.tryClaim(indexId, generation, "owner-B", Instant.now()))
                .as("Concurrent claim must fail while held").isFalse();

        assertThat(handler.release(indexId, generation, "owner-A")).isTrue();

        assertThat(handler.tryClaim(indexId, generation, "owner-B", Instant.now()))
                .as("Claim must succeed after release").isTrue();
    }

    @Property(tries = 20)
    void duplicateClaimBySameOwnerFails(
            @ForAll("indexIds") String indexId,
            @ForAll("generations") int generation) {

        IndexBuildClaimRepository handler = new ClaimOnlyRepository();

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

    /** Test-only CAS model, never registered as a production backend. */
    static final class ClaimOnlyRepository implements IndexBuildClaimRepository {
        private final ConcurrentHashMap<String, String> claims = new ConcurrentHashMap<>();

        @Override
        public boolean tryClaim(String indexId, int generation, String ownerId, Instant now) {
            return claims.putIfAbsent(indexId + ":" + generation, ownerId) == null;
        }

        @Override
        public boolean release(String indexId, int generation, String ownerId) {
            return claims.remove(indexId + ":" + generation, ownerId);
        }

        @Override
        public boolean heartbeat(String indexId, int generation, String ownerId, Instant now) {
            return ownerId.equals(claims.get(indexId + ":" + generation));
        }
    }
}
