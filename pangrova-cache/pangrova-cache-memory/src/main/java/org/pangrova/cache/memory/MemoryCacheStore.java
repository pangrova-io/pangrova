package org.pangrova.cache.memory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.pangrova.cache.api.CacheKey;
import org.pangrova.cache.api.CacheLookup;
import org.pangrova.cache.api.CacheStore;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

final class MemoryCacheStore implements CacheStore {
    private record ValueKey(CacheKey key, String groupToken, String entryToken) { }

    private record Entry(String group, String key) { }

    private record Value(String text, long ttlNanos) { }

    // An evicted generation gets a fresh random identity, so an old loader can never win by ABA.
    private final Cache<String, String> groups;
    private final Cache<Entry, String> entries;
    private final Cache<ValueKey, Value> values;
    private boolean closed;

    MemoryCacheStore(int limit) {
        groups = Caffeine.newBuilder().maximumSize(limit).build();
        entries = Caffeine.newBuilder().maximumSize(limit).build();
        values = Caffeine.newBuilder().maximumSize(limit).expireAfter(new Expiry<ValueKey, Value>() {
            public long expireAfterCreate(ValueKey key, Value value, long time) { return value.ttlNanos(); }
            public long expireAfterUpdate(ValueKey key, Value value, long time, long remaining) {
                return value.ttlNanos();
            }
            public long expireAfterRead(ValueKey key, Value value, long time, long remaining) { return remaining; }
        }).build();
    }

    @Override public synchronized CacheLookup read(CacheKey key) {
        requireOpen();
        String group = groups.get(key.group(), unused -> UUID.randomUUID().toString());
        String entry = entries.get(new Entry(key.group(), key.entry()), unused -> UUID.randomUUID().toString());
        return new CacheLookup(key, group, entry,
                Optional.ofNullable(values.getIfPresent(new ValueKey(key, group, entry))).map(Value::text));
    }

    @Override public synchronized boolean putIfCurrent(CacheLookup observed, String value, Duration ttl) {
        requireOpen();
        if (ttl.isZero() || ttl.isNegative()) throw new IllegalArgumentException("cache TTL must be positive");
        var key = observed.key();
        if (!observed.groupGeneration().equals(groups.getIfPresent(key.group()))
                || !observed.entryGeneration().equals(entries.getIfPresent(new Entry(key.group(), key.entry())))) {
            return false;
        }
        values.put(new ValueKey(key, observed.groupGeneration(), observed.entryGeneration()),
                new Value(Objects.requireNonNull(value), ttl.toNanos()));
        return true;
    }

    @Override public synchronized void invalidate(String group, String entry) {
        requireOpen();
        entries.invalidate(new Entry(group, entry));
    }

    @Override public synchronized void invalidateGroup(String group) {
        requireOpen();
        groups.invalidate(group);
    }

    @Override public synchronized void close() {
        closed = true;
        values.invalidateAll();
        groups.invalidateAll();
        entries.invalidateAll();
    }

    private void requireOpen() {
        if (closed) throw new IllegalStateException("Memory cache is closed");
    }
}
