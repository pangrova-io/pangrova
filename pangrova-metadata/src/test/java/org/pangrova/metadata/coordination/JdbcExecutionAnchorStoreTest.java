package org.pangrova.metadata.coordination;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.coordination.ExecutionAnchor;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcExecutionAnchorStoreTest {

    private JdbcExecutionAnchorStore store;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("execution_anchor");
        store = new JdbcExecutionAnchorStore(dataSource);
    }

    @Test
    void acquireReleaseAndReacquireUsesNeverReusedToken() {
        ExecutionAnchor first = store.tryAcquire(
                "q-1", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        assertThat(store.tryAcquire(
                "q-1", "b", "b-1", Duration.ofMinutes(1))).isEmpty();
        assertThat(store.release(first)).isTrue();

        ExecutionAnchor second = store.tryAcquire(
                "q-1", "b", "b-1", Duration.ofMinutes(1)).orElseThrow();
        assertThat(second.fencingToken()).isGreaterThan(first.fencingToken());
        assertThat(store.release(first)).isFalse();
        assertThat(store.current("q-1")).contains(second);
    }

    @Test
    void batchRenewIsBoundToExactProcessIncarnation() {
        store.tryAcquire("q-1", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        store.tryAcquire("q-2", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        store.tryAcquire("q-3", "a", "a-2", Duration.ofMinutes(1)).orElseThrow();

        assertThat(store.renewOwned("a", "a-1", Duration.ofMinutes(2))).isEqualTo(2);
        assertThat(store.renewOwned("a", "missing", Duration.ofMinutes(2))).isZero();
    }

    @Test
    void yieldAndBoundedClaimAdvanceEveryExecutionToken() {
        ExecutionAnchor first = store.tryAcquire(
                "q-1", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        ExecutionAnchor second = store.tryAcquire(
                "q-2", "a", "a-1", Duration.ofMinutes(1)).orElseThrow();
        assertThat(store.yieldOwned("a", "a-1")).isEqualTo(2);

        var batchOne = store.claimExpired(
                "b", "b-1", Duration.ofMinutes(1), 1);
        var batchTwo = store.claimExpired(
                "b", "b-1", Duration.ofMinutes(1), 1);

        assertThat(batchOne).hasSize(1);
        assertThat(batchTwo).hasSize(1);
        assertThat(batchOne.get(0).executionId()).isNotEqualTo(batchTwo.get(0).executionId());
        assertThat(batchOne.get(0).fencingToken())
                .isGreaterThan(Math.min(first.fencingToken(), second.fencingToken()));
        assertThat(batchTwo.get(0).fencingToken())
                .isGreaterThan(Math.min(first.fencingToken(), second.fencingToken()));
    }
}
