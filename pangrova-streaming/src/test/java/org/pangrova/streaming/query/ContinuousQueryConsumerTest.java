package org.pangrova.streaming.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ContinuousQueryConsumerTest {

    @Test
    void discoversRegistrationAfterConstructionAndRetiresDroppedQuery() throws Exception {
        var registrations = new ConcurrentHashMap<String, Duration>();
        var executed = new CountDownLatch(1);
        var name = new AtomicReference<String>();
        try (var consumer = new ContinuousQueryConsumer(registrations, query -> {
            name.set(query);
            executed.countDown();
        }, Duration.ofHours(1))) {
            assertThat(consumer.scheduledQueryCount()).isZero();

            registrations.put("late_query", Duration.ofHours(1));
            consumer.scan();
            assertThat(executed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(name.get()).isEqualTo("late_query");
            assertThat(consumer.scheduledQueryCount()).isEqualTo(1);

            registrations.remove("late_query");
            consumer.scan();
            assertThat(consumer.scheduledQueryCount()).isZero();
        }
    }
}
