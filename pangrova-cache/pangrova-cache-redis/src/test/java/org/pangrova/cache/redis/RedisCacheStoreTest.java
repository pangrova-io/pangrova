package org.pangrova.cache.redis;

import org.junit.jupiter.api.Test;
import org.pangrova.cache.api.CacheKey;
import org.pangrova.cache.api.CacheStoreFactory;
import org.pangrova.cache.api.CacheStore;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.ServiceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisCacheStoreTest {
    @Test void independentClientsRejectStaleFillsPreserveOtherNamespacesAndPropagateFailures() throws Exception {
        var factory = ServiceLoader.load(CacheStoreFactory.class).findFirst().orElseThrow();
        assertThat(factory.id()).isEqualTo("redis");
        try (var redis = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379)) {
            redis.start();
            var options = Map.of("url", "redis://" + redis.getHost() + ':' + redis.getMappedPort(6379),
                    "namespace", "test:cluster");
            var key = new CacheKey("c[*]", "s:t", "v:1");
            var sibling = new CacheKey("c[*]", "s:t2", "");
            var ttl = Duration.ofMinutes(3);
            try (var writer = factory.open(options); var reader = factory.open(options);
                 var other = factory.open(Map.of("url", options.get("url"), "namespace", "other"))) {
                var before = writer.read(key);
                assertThat(writer.putIfCurrent(before, "old", ttl)).isTrue();
                assertThat(reader.read(key).value()).contains("old");
                assertThat(other.read(key).value()).isEmpty();
                assertThat(other.putIfCurrent(other.read(key), "isolated", ttl)).isTrue();
                assertThat(writer.putIfCurrent(writer.read(sibling), "sibling", ttl)).isTrue();
                reader.invalidate(key.group(), key.entry());
                assertThat(reader.read(sibling).value()).contains("sibling");
                assertThat(writer.putIfCurrent(before, "stale", ttl)).isFalse();
                var admitted = writer.read(key);
                assertThat(admitted.value()).isEmpty();
                reader.invalidateGroup(key.group());
                assertThat(writer.putIfCurrent(admitted, "stale", ttl)).isFalse();
                assertThat(reader.read(sibling).value()).isEmpty();
                assertThat(writer.putIfCurrent(writer.read(key), "new", ttl)).isTrue();
                assertThat(other.read(key).value()).contains("isolated");
                raceInvalidationWithOldFill(writer, reader);
                writer.close();
                assertThat(reader.read(key).value()).contains("new");
                assertThatThrownBy(() -> writer.read(key)).isInstanceOf(RuntimeException.class);
                redis.stop();
                assertThatThrownBy(() -> reader.read(key)).isInstanceOf(RuntimeException.class);
                assertThatThrownBy(() -> factory.open(options)).isInstanceOf(RuntimeException.class);
            }
        }
    }
    private static void raceInvalidationWithOldFill(CacheStore first, CacheStore second) throws Exception {
        var work = Executors.newFixedThreadPool(2);
        var key = new CacheKey("race", "entry", "revision");
        try {
            for (int i = 0; i < 12; i++) {
                var observed = first.read(key);
                var begin = new CountDownLatch(1);
                var oldFill = work.submit(() -> {
                    begin.await();
                    return first.putIfCurrent(observed, "obsolete", Duration.ofSeconds(10));
                });
                var reset = work.submit(() -> {
                    begin.await();
                    second.invalidateGroup(key.group());
                    return second.putIfCurrent(second.read(key), "current", Duration.ofSeconds(10));
                });
                begin.countDown();
                assertThat(reset.get(5, TimeUnit.SECONDS)).isTrue();
                oldFill.get(5, TimeUnit.SECONDS);
                assertThat(first.read(key).value()).contains("current");
            }
        } finally {
            work.shutdownNow();
        }
    }

}
