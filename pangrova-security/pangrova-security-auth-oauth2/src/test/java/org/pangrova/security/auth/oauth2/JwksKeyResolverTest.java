package org.pangrova.security.auth.oauth2;

import java.net.http.HttpClient;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;

import io.jsonwebtoken.security.Jwks;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies JwksKeyResolver Caffeine cache behavior:
 * expireAfterWrite(30min), maximumSize(1000), and revoked key expiration.
 */
class JwksKeyResolverTest {

    private static final long THIRTY_MINUTES_NANOS = TimeUnit.MINUTES.toNanos(30);

    private final AtomicLong fakeTime = new AtomicLong(0);
    private final Ticker fakeTicker = fakeTime::get;
    private JwksKeyResolver resolver;

    @BeforeEach
    void setUp() {
        Cache<String, PublicKey> testCache = Caffeine.newBuilder()
                .expireAfterWrite(JwksKeyResolver.EXPIRE_AFTER_WRITE_MINUTES, TimeUnit.MINUTES)
                .maximumSize(JwksKeyResolver.MAXIMUM_SIZE)
                .ticker(fakeTicker)
                .build();
        resolver = new JwksKeyResolver(HttpClient.newHttpClient(), testCache);
    }

    @Test
    void cachedKeyIsReturnedBeforeExpiry() throws Exception {
        PublicKey key = generateRsaPublicKey();
        resolver.jwksCache.put("kid-1", key);

        assertThat(resolver.jwksCache.getIfPresent("kid-1")).isSameAs(key);
    }

    @Test
    void cachedKeyExpiresAfterThirtyMinutes() throws Exception {
        PublicKey key = generateRsaPublicKey();
        resolver.jwksCache.put("kid-1", key);

        // Advance time past the 30-minute TTL
        fakeTime.addAndGet(THIRTY_MINUTES_NANOS + TimeUnit.SECONDS.toNanos(1));

        // Caffeine performs lazy eviction; cleanUp forces it
        resolver.jwksCache.cleanUp();

        assertThat(resolver.jwksCache.getIfPresent("kid-1")).isNull();
    }

    @Test
    void revokedKeysExpireAutomatically() throws Exception {
        // Simulate: JWKS endpoint returns key-A, then key-A is revoked (removed from JWKS)
        PublicKey keyA = generateRsaPublicKey();
        resolver.jwksCache.put("kid-revoked", keyA);

        // After TTL, the revoked key should no longer be resolvable
        fakeTime.addAndGet(THIRTY_MINUTES_NANOS + TimeUnit.SECONDS.toNanos(1));
        resolver.jwksCache.cleanUp();

        assertThat(resolver.jwksCache.getIfPresent("kid-revoked")).isNull();
    }

    @Test
    void maximumSizeEvictsOldestEntries() throws Exception {
        long maxSize = JwksKeyResolver.MAXIMUM_SIZE;
        PublicKey publicKey = generateRsaPublicKey();

        // Fill cache to maximum + 1
        for (int i = 0; i <= maxSize; i++) {
            resolver.jwksCache.put("kid-" + i, publicKey);
        }
        resolver.jwksCache.cleanUp();

        assertThat(resolver.jwksCache.estimatedSize()).isLessThanOrEqualTo(maxSize);
    }

    @Test
    void parseAndCacheJwksReplacesAllEntries() throws Exception {
        PublicKey oldKey = generateRsaPublicKey();
        resolver.jwksCache.put("old-kid", oldKey);

        // Parse a JWKS JSON that does NOT contain "old-kid"
        String jwksJson = """
                {"keys":[]}
                """;
        resolver.parseAndCacheJwks(jwksJson);

        // Old key should be evicted (invalidateAll + putAll with empty set)
        assertThat(resolver.jwksCache.getIfPresent("old-kid")).isNull();
    }

    @Test
    void clearInvalidatesAllEntries() throws Exception {
        resolver.jwksCache.put("kid-1", generateRsaPublicKey());
        resolver.jwksCache.put("kid-2", generateRsaPublicKey());

        resolver.clear();

        assertThat(resolver.jwksCache.asMap()).isEmpty();
    }

    @Test
    void defaultCacheConfigurationMatchesSpec() {
        assertThat(JwksKeyResolver.EXPIRE_AFTER_WRITE_MINUTES).isEqualTo(30);
        assertThat(JwksKeyResolver.MAXIMUM_SIZE).isEqualTo(1000);
    }

    @Test
    void resolveSigningKey_unknownKidReturnsNull() throws Exception {
        // Populate cache with a known key
        PublicKey keyA = generateRsaPublicKey();
        resolver.jwksCache.put("kid-A", keyA);

        // Requesting an unknown kid should NOT fallback to kid-A
        PublicKey result = resolver.resolveSigningKey("kid-unknown");
        assertThat(result).isNull();
    }

    @Test
    void resolveSigningKey_nullKidFallsBackToFirstKey() throws Exception {
        // Populate cache with a key
        PublicKey keyA = generateRsaPublicKey();
        resolver.jwksCache.put("kid-A", keyA);

        // Null kid (no kid header in JWT) should still fallback to first available key
        PublicKey result = resolver.resolveSigningKey(null);
        assertThat(result).isNotNull();
    }

    @Test
    void resolveSigningKey_knownKidReturnsCorrectKey() throws Exception {
        PublicKey keyA = generateRsaPublicKey();
        PublicKey keyB = generateRsaPublicKey();
        resolver.jwksCache.put("kid-A", keyA);
        resolver.jwksCache.put("kid-B", keyB);

        assertThat(resolver.resolveSigningKey("kid-A")).isSameAs(keyA);
        assertThat(resolver.resolveSigningKey("kid-B")).isSameAs(keyB);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"use\":\"enc\"", "\"alg\":\"PS256\"", "\"key_ops\":[\"encrypt\"]"})
    void discoveryHonorsDeclaredKeyUsage(String restriction) throws Exception {
        RSAPublicKey key = (RSAPublicKey) generateRsaPublicKey();
        String json = Jwks.json(Jwks.builder().key(key).id("restricted").build());
        resolver.parseAndCacheJwks("{\"keys\":[" + json.substring(0, json.length() - 1) + "," + restriction + "}]}");
        assertThat(resolver.resolveSigningKey("restricted")).isNull();
    }

    @Test
    void duplicateSigningKeyIdsCannotSelectOneOfSeveralAuthorities() throws Exception {
        String first = Jwks.json(Jwks.builder().key((RSAPublicKey) generateRsaPublicKey()).id("duplicate").build());
        String second = Jwks.json(Jwks.builder().key((RSAPublicKey) generateRsaPublicKey()).id("duplicate").build());
        resolver.parseAndCacheJwks("{\"keys\":[" + first + "," + second + "," + first + "]}");
        assertThat(resolver.resolveSigningKey("duplicate")).isNull();
        assertThat(resolver.resolveSigningKey(null)).isNull();
    }

    private static PublicKey generateRsaPublicKey() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        KeyPair pair = gen.generateKeyPair();
        return pair.getPublic();
    }
}
