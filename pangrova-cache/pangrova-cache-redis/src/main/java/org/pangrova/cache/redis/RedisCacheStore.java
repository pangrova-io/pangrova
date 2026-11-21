package org.pangrova.cache.redis;

import org.pangrova.cache.api.CacheKey;
import org.pangrova.cache.api.CacheLookup;
import org.pangrova.cache.api.CacheStore;
import redis.clients.jedis.JedisPooled;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Standalone Redis. Lua captures both fences and a value atomically; stale payloads expire by TTL. */
final class RedisCacheStore implements CacheStore {
    private static final String READ = """
            local group = redis.call('GET', KEYS[1])
            if not group then group = ARGV[1]; redis.call('SET', KEYS[1], group) end
            local entry = redis.call('GET', KEYS[2])
            if not entry then entry = ARGV[2]; redis.call('SET', KEYS[2], entry) end
            redis.call('PEXPIRE', KEYS[1], math.max(redis.call('PTTL', KEYS[1]), 3600000))
            redis.call('PEXPIRE', KEYS[2], math.max(redis.call('PTTL', KEYS[2]), 3600000))
            return {group, entry, redis.call('GET', KEYS[3] .. ':' .. group .. ':' .. entry)}
            """;
    private static final String PUT = """
            if redis.call('GET', KEYS[1]) ~= ARGV[1] or redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
            redis.call('PEXPIRE', KEYS[1], math.max(redis.call('PTTL', KEYS[1]), tonumber(ARGV[3])))
            redis.call('PEXPIRE', KEYS[2], math.max(redis.call('PTTL', KEYS[2]), tonumber(ARGV[3])))
            redis.call('PSETEX', KEYS[3] .. ':' .. ARGV[1] .. ':' .. ARGV[2], ARGV[3], ARGV[4])
            return 1
            """;
    private final JedisPooled client;
    private final String namespace;

    RedisCacheStore(JedisPooled client, String namespace) {
        this.client = client;
        this.namespace = "pangrova:metadata:v1:" + encode(namespace) + ':';
    }

    @Override public CacheLookup read(CacheKey key) {
        var result = (List<?>) client.eval(READ, keys(key),
                List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        return new CacheLookup(key, (String) result.get(0), (String) result.get(1),
                Optional.ofNullable((String) result.get(2)));
    }

    @Override public boolean putIfCurrent(CacheLookup observed, String value, Duration ttl) {
        long millis = ttl.toMillis();
        if (millis <= 0) throw new IllegalArgumentException("Redis cache TTL must be at least one millisecond");
        return Long.valueOf(1).equals(client.eval(PUT, keys(observed.key()), List.of(
                observed.groupGeneration(), observed.entryGeneration(), Long.toString(millis), value)));
    }

    @Override public void invalidate(String group, String entry) { client.del(entryKey(group, entry)); }
    @Override public void invalidateGroup(String group) { client.del(groupKey(group)); }
    @Override public void close() { client.close(); }

    private List<String> keys(CacheKey key) {
        String entry = entryKey(key.group(), key.entry());
        return List.of(groupKey(key.group()), entry, entry + ":v:" + encode(key.variant()));
    }
    private String entryKey(String group, String entry) { return groupKey(group) + ":e:" + encode(entry); }
    private String groupKey(String group) { return namespace + "g:" + encode(group); }
    private static String encode(String text) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }
}
