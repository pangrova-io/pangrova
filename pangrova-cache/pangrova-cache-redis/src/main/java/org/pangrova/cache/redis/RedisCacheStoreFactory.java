package org.pangrova.cache.redis;

import org.pangrova.cache.api.CacheStore;
import org.pangrova.cache.api.CacheStoreFactory;
import redis.clients.jedis.JedisPooled;

import java.net.URI;
import java.util.Map;
import java.util.Set;

public final class RedisCacheStoreFactory implements CacheStoreFactory {
    @Override public String id() { return "redis"; }
    @Override public int apiVersion() { return API_VERSION; }

    @Override public CacheStore open(Map<String, String> settings) {
        if (!settings.keySet().equals(Set.of("url", "namespace"))) {
            throw new IllegalArgumentException("Redis cache requires url and namespace");
        }
        final URI url;
        try {
            url = URI.create(settings.get("url"));
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Invalid Redis cache URL");
        }
        if (!("redis".equals(url.getScheme()) || "rediss".equals(url.getScheme())) || url.getHost() == null) {
            throw new IllegalArgumentException("Redis cache URL requires redis or rediss and a host");
        }
        var namespace = settings.get("namespace");
        if (namespace.isBlank()) throw new IllegalArgumentException("Redis cache namespace must be nonblank");
        var client = new JedisPooled(url);
        try {
            if (!"PONG".equals(client.ping())) throw new IllegalStateException("Redis cache did not acknowledge PING");
            return new RedisCacheStore(client, namespace);
        } catch (RuntimeException failure) {
            try {
                client.close();
            } catch (RuntimeException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }
}
