package org.pangrova.worker.buffer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class ShuffleBufferCloseTest {
    @TempDir Path directory;

    @Test
    void closeWakesEveryProducerBlockedOnAFullPartition() throws Exception {
        var key = new ShuffleBufferKey("query", "stage", 0);
        var manager = new ShuffleBufferManager(1);
        manager.createPartition(key);
        manager.addBatch(key, new byte[]{1, 2, 3});
        var threads = new CopyOnWriteArrayList<Thread>();
        var pool = Executors.newFixedThreadPool(16, work -> {
            var thread = new Thread(work, "shuffle-blocked-producer-" + threads.size());
            threads.add(thread);
            return thread;
        });
        try {
            var producers = new ArrayList<Future<?>>();
            for (int index = 0; index < 16; index++) {
                producers.add(pool.submit(() -> {
                    manager.addBatch(key, new byte[]{4});
                    return null;
                }));
            }
            await().atMost(Duration.ofSeconds(5)).until(() -> threads.size() == 16
                    && threads.stream().allMatch(thread -> thread.getState() == Thread.State.WAITING));
            assertThat(producers).allSatisfy(future -> assertThat(future.isDone()).isFalse());
            assertThat(manager.getTotalBufferedBytes()).isEqualTo(3);
            manager.close();
            for (var producer : producers) {
                assertThatThrownBy(() -> producer.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(IllegalStateException.class);
            }
            assertThat(manager.getTotalBufferedBytes()).isZero();
            assertThat(manager.pollBatch(key, 0)).isNull();
            assertThatThrownBy(() -> manager.createPartition(new ShuffleBufferKey("other", "stage", 1)))
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            manager.close();
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void closeWakesEveryConsumerWithoutPublishingSentinelData() throws Exception {
        var key = new ShuffleBufferKey("query", "stage", 0);
        var manager = new ShuffleBufferManager(1);
        manager.createPartition(key);
        var threads = new CopyOnWriteArrayList<Thread>();
        var pool = Executors.newFixedThreadPool(8, work -> {
            var thread = new Thread(work, "shuffle-blocked-consumer-" + threads.size());
            threads.add(thread);
            return thread;
        });
        try {
            List<Future<byte[]>> consumers = new ArrayList<>();
            for (int index = 0; index < 8; index++) {
                consumers.add(pool.submit(() -> manager.pollBatch(key, TimeUnit.MINUTES.toMillis(1))));
            }
            await().atMost(Duration.ofSeconds(5)).until(() -> threads.size() == 8
                    && threads.stream().allMatch(thread -> thread.getState() == Thread.State.TIMED_WAITING));
            manager.close();
            for (var consumer : consumers) {
                assertThat(consumer.get(5, TimeUnit.SECONDS)).isNull();
            }
            assertThat(manager.getTotalBufferedBytes()).isZero();
        } finally {
            manager.close();
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void actualDiskSpillReturnsOnlyTheDequeuedBytes() throws Exception {
        var key = new ShuffleBufferKey("query", "stage", 0);
        var spiller = new ShuffleDiskSpiller(directory.resolve("spill"));
        try (var manager = new ShuffleBufferManager(4, spiller, 0.5, 16)) {
            manager.createPartition(key);
            byte[] payload = {1, 2, 3, 4, 5, 6, 7, 8, 9};
            manager.addBatch(key, payload);
            assertThat(manager.getSpillCount()).isEqualTo(1);
            assertThat(manager.getTotalBufferedBytes()).isZero();
            assertThat(spiller.totalSpillBytes()).isEqualTo(payload.length);
            assertThat(spiller.totalSpillFiles()).isEqualTo(1);
            assertThat(spiller.read(key.toString().hashCode() & Integer.MAX_VALUE, 0)).isEqualTo(payload);
            assertThat(manager.pollBatch(key, 0)).isNull();
        }
        assertThat(directory.resolve("spill")).doesNotExist();
    }
}
