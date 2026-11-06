package org.pangrova.coordinator.query.cache;

import org.pangrova.planner.core.cache.CachedResult;
import org.pangrova.planner.core.cache.L2CacheStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPooled;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;

/**
 * Redis-backed L2 cache store for cross-coordinator query result sharing.
 * Uses Jedis pooled client for thread-safe Redis access.
 *
 * <p>Cache entries are stored as raw bytes with configurable TTL.
 * Key format: {@code qrc:<sha256-hash>} (query result cache prefix).
 * Value format: 8-byte epoch-millis timestamp + result bytes.</p>
 */
public final class RedisL2CacheStore implements L2CacheStore, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RedisL2CacheStore.class);
    private static final String KEY_PREFIX = "qrc:";

    private final JedisPooled jedis;
    private final long ttlSeconds;
    private final long maxEntryBytes;

    /**
     * @param redisUrl      Redis connection URL (e.g. "redis://localhost:6379")
     * @param ttlSeconds    time-to-live for cache entries in seconds
     * @param maxEntryBytes maximum size of a single cache entry in bytes (0 = unlimited)
     */
    public RedisL2CacheStore(String redisUrl, long ttlSeconds, long maxEntryBytes) {
        this.jedis = new JedisPooled(redisUrl);
        this.ttlSeconds = ttlSeconds;
        this.maxEntryBytes = maxEntryBytes;
        LOG.info("event=redis_l2_cache_init url={} ttlSeconds={} maxEntryBytes={}",
                redisUrl, ttlSeconds, maxEntryBytes);
    }

    @Override
    public Optional<CachedResult> get(String key) {
        try {
            byte[] raw = jedis.get(redisKey(key));
            if (raw == null || raw.length < 8) {
                return Optional.empty();
            }
            long epochMillis = bytesToLong(raw, 0);
            byte[] data = new byte[raw.length - 8];
            System.arraycopy(raw, 8, data, 0, data.length);
            return Optional.of(new CachedResult(data, Instant.ofEpochMilli(epochMillis)));
        } catch (Exception e) {
            LOG.warn("event=redis_l2_get_failed key={} error={}", key, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public void put(String key, CachedResult value) {
        if (value == null || value.data() == null) {
            return;
        }
        if (maxEntryBytes > 0 && value.data().length > maxEntryBytes) {
            LOG.debug("event=redis_l2_skip_large key={} size={} max={}",
                    key, value.data().length, maxEntryBytes);
            return;
        }
        try {
            byte[] raw = new byte[8 + value.data().length];
            longToBytes(value.cachedAt().toEpochMilli(), raw, 0);
            System.arraycopy(value.data(), 0, raw, 8, value.data().length);
            jedis.setex(redisKey(key), ttlSeconds, raw);
        } catch (Exception e) {
            LOG.warn("event=redis_l2_put_failed key={} error={}", key, e.getMessage());
        }
    }

    @Override
    public void remove(String key) {
        try {
            jedis.del(redisKey(key));
        } catch (Exception e) {
            LOG.warn("event=redis_l2_del_failed key={} error={}", key, e.getMessage());
        }
    }

    @Override
    public void removeByTable(String tableName) {
        scanAndDelete("*" + tableName.toLowerCase() + "*");
    }

    @Override
    public void removeByUser(String userId) {
        scanAndDelete("*" + userId + "*");
    }

    @Override
    public void close() {
        jedis.close();
        LOG.info("event=redis_l2_cache_closed");
    }

    private byte[] redisKey(String key) {
        return (KEY_PREFIX + key).getBytes(StandardCharsets.UTF_8);
    }

    private void scanAndDelete(String pattern) {
        try {
            var params = new redis.clients.jedis.params.ScanParams()
                    .match(KEY_PREFIX + pattern).count(100);
            byte[] cursor = "0".getBytes(StandardCharsets.UTF_8);
            do {
                var result = jedis.scan(cursor, params);
                for (byte[] k : result.getResult()) {
                    jedis.del(k);
                }
                cursor = result.getCursorAsBytes();
            } while (!java.util.Arrays.equals(cursor, "0".getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            LOG.warn("event=redis_l2_scan_delete_failed pattern={} error={}",
                    pattern, e.getMessage());
        }
    }

    private static long bytesToLong(byte[] bytes, int offset) {
        return ByteBuffer.wrap(bytes, offset, Long.BYTES).getLong();
    }

    private static void longToBytes(long value, byte[] bytes, int offset) {
        ByteBuffer.wrap(bytes, offset, Long.BYTES).putLong(value);
    }
}
