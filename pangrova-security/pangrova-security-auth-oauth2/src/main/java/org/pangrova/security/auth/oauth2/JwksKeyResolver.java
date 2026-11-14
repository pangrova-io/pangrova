package org.pangrova.security.auth.oauth2;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.PublicKey;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.jsonwebtoken.security.Jwk;
import io.jsonwebtoken.security.Jwks;
import io.jsonwebtoken.security.PublicJwk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers and caches JWKS public keys from an OIDC issuer.
 *
 * <p>Performs OIDC well-known discovery to locate the JWKS endpoint,
 * then fetches and caches RS256 and ES256 verification keys by key ID (kid).</p>
 *
 * <p>BUG-S02: Cache is replaced atomically on refresh — revoked keys are evicted.</p>
 * <p>BUG-S03: Concurrent cache-miss refreshes are coalesced via a shared CompletableFuture.</p>
 *
 * @ThreadSafe all mutable state is accessed via AtomicReference or Caffeine Cache
 */
final class JwksKeyResolver {

    private static final Logger LOG = LoggerFactory.getLogger(JwksKeyResolver.class);
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(5);
    private static final int HTTP_SUCCESS_MINIMUM = 200;
    private static final int HTTP_REDIRECT_MINIMUM = 300;
    static final long EXPIRE_AFTER_WRITE_MINUTES = 30;
    static final long MAXIMUM_SIZE = 1000;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    /** Allows injecting JWKS keys directly. */
    @org.pangrova.common.VisibleForTesting
    final Cache<String, PublicKey> jwksCache;
    private final AtomicReference<String> jwksEndpointRef = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<Void>> pendingRefresh = new AtomicReference<>();
    private final HttpClient httpClient;
    /**
     * When true, JWTs without a {@code kid} header are rejected outright (strict
     * defense-in-depth for deployments that mandate key identification), even when
     * the JWKS currently holds a single key. Default false preserves compatibility
     * with single-key OIDC providers that legitimately omit {@code kid} (F-SEC-08).
     */
    private final boolean requireKid;
    /** Counts JWTs presented without a kid header, for observability/alerting (F-SEC-08). */
    private final AtomicLong missingKidCount = new AtomicLong(0);

    JwksKeyResolver(HttpClient httpClient) {
        this(httpClient, buildDefaultCache(), false);
    }

    JwksKeyResolver(HttpClient httpClient, Cache<String, PublicKey> cache) {
        this(httpClient, cache, false);
    }

    JwksKeyResolver(HttpClient httpClient, Cache<String, PublicKey> cache, boolean requireKid) {
        this.httpClient = httpClient;
        this.jwksCache = cache;
        this.requireKid = requireKid;
    }

    private static Cache<String, PublicKey> buildDefaultCache() {
        return Caffeine.newBuilder()
                .expireAfterWrite(EXPIRE_AFTER_WRITE_MINUTES, TimeUnit.MINUTES)
                .maximumSize(MAXIMUM_SIZE)
                .build();
    }

    void discoverJwksEndpoint(String issuerUri) {
        try {
            String wellKnown = buildWellKnownUrl(issuerUri);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(wellKnown))
                    .timeout(HTTP_TIMEOUT)
                    .GET()
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= HTTP_SUCCESS_MINIMUM
                    && resp.statusCode() < HTTP_REDIRECT_MINIMUM) {
                Map<String, Object> discovery = parseJson(resp.body());
                String jwksUri = extractString(discovery, "jwks_uri");
                if (jwksUri != null) {
                    jwksEndpointRef.set(jwksUri);
                    LOG.info("event=oauth2_jwks_discovered jwksUri={}", jwksUri);
                    refreshJwks(jwksUri);
                }
            }
        } catch (Exception e) {
            LOG.warn("event=oauth2_jwks_discovery_failed issuerUri={} error={}", issuerUri, e.getMessage());
        }
    }

    private void refreshJwks(String jwksUri) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(jwksUri))
                    .timeout(HTTP_TIMEOUT)
                    .GET()
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= HTTP_SUCCESS_MINIMUM
                    && resp.statusCode() < HTTP_REDIRECT_MINIMUM) {
                parseAndCacheJwks(resp.body());
            }
        } catch (Exception e) {
            LOG.warn("event=oauth2_jwks_refresh_failed jwksUri={} error={}", jwksUri, e.getMessage());
        }
    }

    synchronized void parseAndCacheJwks(String jwksJson) {
        Map<String, Object> jwks = parseJson(jwksJson);
        Object keysObj = jwks.get("keys");
        if (!(keysObj instanceof List<?> keys)) {
            return;
        }
        // BUG-S02: Build a fresh map and replace atomically so revoked keys are evicted.
        Map<String, PublicKey> newEntries = new HashMap<>();
        Set<String> ambiguous = new HashSet<>();
        for (Object keyObj : keys) {
            if (!(keyObj instanceof Map<?, ?> key) || !(key.get("kid") instanceof String kid)
                    || kid.isBlank() || ambiguous.contains(kid)) {
                continue;
            }
            PublicKey verified = verificationKey(key);
            if (verified != null && newEntries.putIfAbsent(kid, verified) != null) {
                ambiguous.add(kid);
                newEntries.remove(kid);
            }
        }
        jwksCache.invalidateAll();
        jwksCache.putAll(newEntries);
        LOG.info("event=oauth2_jwks_refreshed keyCount={}", newEntries.size());
    }

    private static PublicKey verificationKey(Map<?, ?> key) {
        String algorithm;
        if ("RSA".equals(key.get("kty"))) {
            algorithm = "RS256";
        } else if ("EC".equals(key.get("kty")) && "P-256".equals(key.get("crv"))) {
            algorithm = "ES256";
        } else {
            return null;
        }
        if (key.containsKey("use") && !"sig".equals(key.get("use"))
                || key.containsKey("alg") && !algorithm.equals(key.get("alg"))
                || key.containsKey("key_ops") && (!(key.get("key_ops") instanceof List<?> operations)
                        || !operations.contains("verify"))) {
            return null;
        }
        try {
            Jwk<?> jwk = Jwks.parser().build().parse(JSON.writeValueAsString(key));
            return jwk instanceof PublicJwk<?> publicJwk ? publicJwk.toKey() : null;
        } catch (Exception e) {
            LOG.warn("event=oauth2_jwk_parse_failed kid={} errorType={}", key.get("kid"), e.getClass().getSimpleName());
            return null;
        }
    }

    /**
     * Resolves the signing key for the given kid, refreshing JWKS if needed.
     *
     * <p>If {@code kid} is present in the JWT header but not found in the cache
     * (even after a refresh), returns {@code null} to force an authentication failure.
     * Falling back to an arbitrary key when a specific kid is requested would allow
     * an attacker to influence verification by manipulating JWKS ordering.</p>
     *
     * <p>If {@code kid} is null (JWT has no kid header) and exactly one key exists
     * in the JWKS cache, uses that key as a best-effort for single-key OIDC providers.
     * If multiple keys exist and no kid is specified, returns {@code null} to prevent
     * ambiguous key selection (F-SEC-08).</p>
     */
    PublicKey resolveSigningKey(String kid) {
        if (kid != null) {
            PublicKey resolved = resolveByKid(kid);
            if (resolved != null) {
                return resolved;
            }
            LOG.warn("event=oauth2_unknown_signing_key kid={}", kid);
            return null; // Fail authentication — kid not in JWKS after refresh
        }
        // No kid header. Count for observability/alerting, then decide per policy.
        missingKidCount.incrementAndGet();
        if (requireKid) {
            LOG.warn("event=oauth2_jwt_missing_kid outcome=rejected mode=require_kid"
                    + " hint=strict mode mandates a kid header; token rejected");
            return null;
        }
        // No kid header — only safe to use single-key fallback
        Map<String, PublicKey> snapshot = snapshot();
        if (snapshot.size() == 1) {
            LOG.warn("event=oauth2_jwt_missing_kid fallback=single_key_provider"
                    + " hint=JWT has no kid header; using sole available key");
            return snapshot.values().iterator().next();
        }
        if (snapshot.isEmpty()) {
            LOG.warn("event=oauth2_jwt_missing_kid outcome=no_keys_available");
        } else {
            LOG.warn("event=oauth2_jwt_missing_kid outcome=ambiguous_key_selection"
                    + " keyCount={} hint=JWT must include kid header when JWKS has multiple keys",
                    snapshot.size());
        }
        return null;
    }

    private synchronized Map<String, PublicKey> snapshot() {
        return Map.copyOf(jwksCache.asMap());
    }

    private synchronized PublicKey cachedKey(String kid) {
        return jwksCache.getIfPresent(kid);
    }

    private PublicKey resolveByKid(String kid) {
        PublicKey cached = cachedKey(kid);
        if (cached != null) {
            return cached;
        }
        String jwksUri = jwksEndpointRef.get();
        if (jwksUri != null) {
            // BUG-S03: Coalesce concurrent refreshes — only one thread fetches,
            // others wait on the same future to avoid thundering herd.
            coalescedRefresh(jwksUri);
            return cachedKey(kid);
        }
        return null;
    }

    /**
     * Coalesces concurrent JWKS refresh requests. The first caller creates a
     * CompletableFuture and performs the HTTP fetch; subsequent callers join
     * the same future instead of issuing duplicate requests.
     */
    private void coalescedRefresh(String jwksUri) {
        CompletableFuture<Void> existing = pendingRefresh.get();
        if (existing != null) {
            joinQuietly(existing);
            return;
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (pendingRefresh.compareAndSet(null, future)) {
            executeRefreshFuture(jwksUri, future);
        } else {
            joinQuietly(pendingRefresh.get());
        }
    }

    private void executeRefreshFuture(String jwksUri, CompletableFuture<Void> future) {
        try {
            refreshJwks(jwksUri);
            future.complete(null);
        } catch (Exception e) {
            future.completeExceptionally(e);
        } finally {
            pendingRefresh.compareAndSet(future, null);
        }
    }

    private void joinQuietly(CompletableFuture<Void> future) {
        if (future == null) {
            return;
        }
        try {
            future.join();
        } catch (Exception e) {
            LOG.debug("event=oauth2_jwks_coalesced_join_failed error={}", e.getMessage());
        }
    }

    synchronized void clear() {
        jwksCache.invalidateAll();
    }

    /** Number of JWTs seen without a kid header (observability for F-SEC-08). */
    @org.pangrova.common.VisibleForTesting
    long missingKidCount() {
        return missingKidCount.get();
    }

    static String buildWellKnownUrl(String issuerUri) {
        return issuerUri.endsWith("/")
                ? issuerUri + ".well-known/openid-configuration"
                : issuerUri + "/.well-known/openid-configuration";
    }

    private static String extractString(Map<String, Object> map, String key) {
        Object val = map.get(key);
        return val != null ? val.toString() : null;
    }

    private static Map<String, Object> parseJson(String json) {
        try {
            return JSON.readValue(json, MAP_TYPE);
        } catch (Exception e) {
            LOG.warn("event=json_parse_failed error={}", e.getMessage());
            return Map.of();
        }
    }
}
