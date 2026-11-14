package org.pangrova.security.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

import io.jsonwebtoken.security.Jwks;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.pangrova.security.api.authentication.AuthenticationRequest;
import org.pangrova.security.api.authentication.AuthenticationResult;

/**
 * Tests OAuth2Authenticator JWT validation logic with real token structures.
 * No mocks — uses real JWT tokens with known payloads and RSA signatures.
 */
class OAuth2AuthenticatorTest {

    private static String issuer;
    private static com.sun.net.httpserver.HttpServer identityServer;
    private static final String KID = "test-kid";

    private static KeyPair rsaKeyPair;
    private static KeyPair ecKeyPair;

    @BeforeAll
    static void generateKeys() throws Exception {
        rsaKeyPair = generateRsaKeyPair();
        KeyPairGenerator ecGenerator = KeyPairGenerator.getInstance("EC");
        ecGenerator.initialize(new ECGenParameterSpec("secp256r1"));
        ecKeyPair = ecGenerator.generateKeyPair();
        identityServer = com.sun.net.httpserver.HttpServer.create(
                new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        issuer = "http://127.0.0.1:" + identityServer.getAddress().getPort();
        identityServer.createContext("/.well-known/openid-configuration", exchange -> {
            byte[] body = ("{\"jwks_uri\":\"" + issuer + "/jwks\"}")
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        identityServer.createContext("/jwks", exchange -> {
            String rsa = Jwks.json(Jwks.builder().key((RSAPublicKey) rsaKeyPair.getPublic()).id(KID).build());
            String ec = Jwks.json(Jwks.builder().key((ECPublicKey) ecKeyPair.getPublic()).id("ec-key").build());
            byte[] body = ("{\"keys\":[" + rsa + "," + ec + "]}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) {
                output.write(body);
            }
        });
        identityServer.start();
    }

    @AfterAll
    static void closeIdentityServer() {
        identityServer.stop(0);
    }

    // --- Claims validation tests (using RS256-signed tokens) ---

    @Test
    void validToken_authenticatesSuccessfully() throws Exception {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String token = buildSignedJwt(issuer, "alice", Instant.now().plusSeconds(3600),
                    KID, rsaKeyPair.getPrivate());
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isTrue();
            assertThat(result.userIdentity()).isNotNull();
            assertThat(result.userIdentity().principal()).isEqualTo("alice");
        }
    }

    @Test
    void expiredToken_isRejected() throws Exception {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String token = buildSignedJwt(issuer, "bob", Instant.now().minusSeconds(60),
                    KID, rsaKeyPair.getPrivate());
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("expired");
        }
    }

    @Test
    void wrongIssuer_isRejected() throws Exception {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String token = buildSignedJwt("https://evil.com", "mallory", Instant.now().plusSeconds(3600),
                    KID, rsaKeyPair.getPrivate());
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("issuer mismatch");
        }
    }

    @Test
    void tokenWithGroups_extractsGroups() throws Exception {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            long exp = Instant.now().plusSeconds(3600).getEpochSecond();
            String payload = String.format(
                    "{\"sub\":\"carol\",\"iss\":\"%s\",\"exp\":%d,\"groups\":[\"admin\",\"dev\"]}",
                    issuer, exp);
            String token = buildSignedJwtFromPayload(payload, KID, rsaKeyPair.getPrivate());

            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isTrue();
            assertThat(result.userIdentity().groups()).containsExactlyInAnyOrder("admin", "dev");
        }
    }

    @Test
    void tokenWithRoles_extractsAsGroups() throws Exception {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            long exp = Instant.now().plusSeconds(3600).getEpochSecond();
            String payload = String.format(
                    "{\"sub\":\"dave\",\"iss\":\"%s\",\"exp\":%d,\"roles\":\"analyst,viewer\"}",
                    issuer, exp);
            String token = buildSignedJwtFromPayload(payload, KID, rsaKeyPair.getPrivate());

            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isTrue();
            assertThat(result.userIdentity().groups()).containsExactlyInAnyOrder("analyst", "viewer");
        }
    }

    @Test
    void malformedToken_isRejected() {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, "not-a-jwt", null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
        }
    }

    @Test
    void tokenMissingSubject_isRejected() throws Exception {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            long exp = Instant.now().plusSeconds(3600).getEpochSecond();
            String payload = String.format("{\"iss\":\"%s\",\"exp\":%d}", issuer, exp);
            String token = buildSignedJwtFromPayload(payload, KID, rsaKeyPair.getPrivate());

            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("subject");
        }
    }

    @Test
    void tokenMissingExpiration_isRejected() throws Exception {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String payload = String.format("{\"sub\":\"eve\",\"iss\":\"%s\"}", issuer);
            String token = buildSignedJwtFromPayload(payload, KID, rsaKeyPair.getPrivate());

            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("expiration");
        }
    }

    @Test
    void missingIssuerIsRejectedDuringInitialization() {
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            assertThatThrownBy(() -> auth.init(Map.of()))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("issuerUri");
            assertThat(auth.isHealthy()).isFalse();
        }
    }

    // --- Signature verification tests ---

    @Test
    void rsaSignedToken_authenticatesSuccessfully() throws Exception {
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            auth.init(signatureRequiredConfig());

            String token = buildSignedJwt(issuer, "alice", Instant.now().plusSeconds(3600),
                    KID, rsaKeyPair.getPrivate());
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isTrue();
            assertThat(result.userIdentity().principal()).isEqualTo("alice");
        }
    }

    @Test
    void rsaSignedToken_wrongKey_isRejected() throws Exception {
        KeyPair signingKey = generateRsaKeyPair();
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            auth.init(signatureRequiredConfig());

            String token = buildSignedJwt(issuer, "mallory", Instant.now().plusSeconds(3600),
                    KID, signingKey.getPrivate());
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("signature verification failed");
        }
    }

    @Test
    void signedToken_noKeyAvailable_rejectedWhenRequired() throws Exception {
        KeyPair keyPair = generateRsaKeyPair();
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            auth.init(signatureRequiredConfig());

            String token = buildSignedJwt(issuer, "alice", Instant.now().plusSeconds(3600),
                    "unknown-kid", keyPair.getPrivate());
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("No signing key available");
        }
    }

    // --- Algorithm validation tests [F-SEC-004] ---

    @Test
    void tokenWithAlgNone_isRejectedWithSpecificErrorCode() {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String token = buildTokenWithAlgorithm("none", issuer, "attacker",
                    Instant.now().plusSeconds(3600));
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains(JwtAlgorithmValidator.ERROR_ALG_NONE);
            assertThat(result.errorMessage()).contains("alg=none");
        }
    }

    @Test
    void tokenWithAlgNoneUpperCase_isRejected() {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String token = buildTokenWithAlgorithm("NONE", issuer, "attacker",
                    Instant.now().plusSeconds(3600));
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains(JwtAlgorithmValidator.ERROR_ALG_NONE);
        }
    }

    @Test
    void tokenWithAlgHS256_isRejectedAsSymmetric() {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String token = buildTokenWithAlgorithm("HS256", issuer, "attacker",
                    Instant.now().plusSeconds(3600));
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains(JwtAlgorithmValidator.ERROR_ALG_SYMMETRIC);
            assertThat(result.errorMessage()).contains("HS256");
        }
    }

    @Test
    void tokenWithAlgHS384_isRejectedAsSymmetric() {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String token = buildTokenWithAlgorithm("HS384", issuer, "attacker",
                    Instant.now().plusSeconds(3600));
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains(JwtAlgorithmValidator.ERROR_ALG_SYMMETRIC);
        }
    }

    @Test
    void tokenWithAlgHS512_isRejectedAsSymmetric() {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String token = buildTokenWithAlgorithm("HS512", issuer, "attacker",
                    Instant.now().plusSeconds(3600));
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains(JwtAlgorithmValidator.ERROR_ALG_SYMMETRIC);
        }
    }

    @Test
    void tokenWithUnknownAlgorithm_isRejected() {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String token = buildTokenWithAlgorithm("PS256", issuer, "attacker",
                    Instant.now().plusSeconds(3600));
            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains(JwtAlgorithmValidator.ERROR_ALG_NOT_ALLOWED);
        }
    }

    @Test
    void signatureBypassConfigurationIsRejected() {
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            assertThatThrownBy(() -> auth.init(Map.of(
                    "issuerUri", issuer, "requireSignatureVerification", "false")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("signatures are mandatory");
        }
    }

    // --- Audience validation tests ---

    @Test
    void tokenWithCorrectAudience_authenticatesSuccessfully() throws Exception {
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            auth.init(Map.of(
                    "issuerUri", issuer,
                    "audience", "my-app"));

            long exp = Instant.now().plusSeconds(3600).getEpochSecond();
            String payload = String.format(
                    "{\"sub\":\"alice\",\"iss\":\"%s\",\"exp\":%d,\"aud\":\"my-app\"}",
                    issuer, exp);
            String token = buildSignedJwtFromPayload(payload, KID, rsaKeyPair.getPrivate());

            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"other-app\"", "\"my-app-admin\"", "\"MY-APP\"",
            "[\"other,my-app\"]", "[\"other\",\"not-my-app\"]"})
    void tokenWithWrongAudience_isRejected(String audienceJson) throws Exception {
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            auth.init(Map.of("issuerUri", issuer, "audience", "my-app"));
            String payload = String.format(
                    "{\"sub\":\"alice\",\"iss\":\"%s\",\"exp\":%d,\"aud\":%s}",
                    issuer, Instant.now().plusSeconds(3600).getEpochSecond(), audienceJson);
            String token = buildSignedJwtFromPayload(payload, KID, rsaKeyPair.getPrivate());
            AuthenticationResult result = authenticate(auth, token);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("audience mismatch");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"plain", "DEF", "GZIP"})
    void signedClaimsUseTheSameExpandedByteLimit(String format) {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String json = "{\"sub\":\"alice\",\"iss\":\"" + issuer + "\",\"exp\":"
                    + Instant.now().plusSeconds(3600).getEpochSecond() + ",\"padding\":\"";
            int limit = SecureJwtParser.MAX_PAYLOAD_DECOMPRESSION_BYTES;
            String prefix = json + "x".repeat(limit - json.length() - 2);
            for (int extraBytes = 0; extraBytes <= 1; extraBytes++) {
                String payload = prefix + "x".repeat(extraBytes) + "\"}";
                JwtBuilder builder = Jwts.builder().header().keyId(KID).and().content(payload);
                if (format.equals("DEF")) {
                    builder.compressWith(Jwts.ZIP.DEF);
                } else if (format.equals("GZIP")) {
                    builder.compressWith(Jwts.ZIP.GZIP);
                }
                String token = builder.signWith(rsaKeyPair.getPrivate(), Jwts.SIG.RS256).compact();
                AuthenticationResult result = authenticate(auth, token);
                assertThat(result.success()).as("%s, %s bytes: %s", format, payload.length(), result.errorMessage())
                        .isEqualTo(extraBytes == 0);
                if (extraBytes > 0) {
                    assertThat(result.errorMessage()).contains("size limit");
                }
            }
        }
    }

    private static AuthenticationResult authenticate(OAuth2Authenticator auth, String token) {
        return auth.authenticate(new AuthenticationRequest(null, null, token, null, Map.of(), "127.0.0.1"));
    }

    @Test
    void tokenWithAudienceArray_matchesCorrectly() throws Exception {
        try (OAuth2Authenticator auth = new OAuth2Authenticator()) {
            auth.init(Map.of(
                    "issuerUri", issuer,
                    "audience", "my-app"));

            long exp = Instant.now().plusSeconds(3600).getEpochSecond();
            String payload = String.format(
                    "{\"sub\":\"alice\",\"iss\":\"%s\",\"exp\":%d,\"aud\":[\"other-app\",\"my-app\"]}",
                    issuer, exp);
            String token = buildSignedJwtFromPayload(payload, KID, rsaKeyPair.getPrivate());

            AuthenticationRequest request = new AuthenticationRequest(
                    null, null, token, null, Map.of(), "10.0.0.1");

            AuthenticationResult result = auth.authenticate(request);
            assertThat(result.success()).isTrue();
        }
    }

    @Test
    void nonStringGroupCannotBecomeAGroupName() throws Exception {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String payload = "{\"sub\":\"alice\",\"iss\":\"" + issuer + "\",\"exp\":"
                    + Instant.now().plusSeconds(60).getEpochSecond() + ",\"groups\":[7]}";
            AuthenticationResult result = authenticate(auth, buildSignedJwtFromPayload(payload, KID, rsaKeyPair.getPrivate()));
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("group names");
        }
    }

    @Test
    void discoveredEcKeyVerifiesSignedClaims() {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String token = Jwts.builder().header().keyId("ec-key").and().issuer(issuer).subject("ec-user")
                    .expiration(java.util.Date.from(Instant.now().plus(Duration.ofMinutes(5))))
                    .signWith(ecKeyPair.getPrivate(), Jwts.SIG.ES256).compact();
            assertThat(authenticate(auth, token).userIdentity().principal()).isEqualTo("ec-user");
        }
    }

    @Test
    void unavailableReplacementIssuerCannotReusePreviousSigningKeys() throws Exception {
        try (OAuth2Authenticator auth = createAuthWithKey()) {
            String secondIssuer = issuer + "/unavailable";
            auth.init(Map.of("issuerUri", secondIssuer));
            String token = buildSignedJwt(secondIssuer, "attacker", Instant.now().plusSeconds(60),
                    KID, rsaKeyPair.getPrivate());
            AuthenticationResult result = authenticate(auth, token);
            assertThat(result.success()).isFalse();
            assertThat(result.errorMessage()).contains("No signing key");
        }
    }

    private static OAuth2Authenticator createAuthWithKey() {
        OAuth2Authenticator auth = new OAuth2Authenticator();
        auth.init(signatureRequiredConfig());
        return auth;
    }

    private static Map<String, String> signatureRequiredConfig() {
        return Map.of("issuerUri", issuer);
    }

    private static KeyPair generateRsaKeyPair() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        return gen.generateKeyPair();
    }

    /**
     * Builds a token with a specific algorithm header but no valid signature.
     * Used to test algorithm validation which happens before signature verification.
     */
    private static String buildTokenWithAlgorithm(String alg, String issuer, String subject,
                                                   Instant expiry) {
        String header = String.format("{\"alg\":\"%s\",\"typ\":\"JWT\"}", alg);
        String payload = String.format(
                "{\"sub\":\"%s\",\"iss\":\"%s\",\"exp\":%d}",
                subject, issuer, expiry.getEpochSecond());
        String encodedHeader = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(header.getBytes(StandardCharsets.UTF_8));
        String encodedPayload = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        return encodedHeader + "." + encodedPayload + ".fake-signature";
    }

    private static String buildSignedJwt(String issuer, String subject, Instant expiry,
                                         String kid, PrivateKey privateKey) throws Exception {
        String payload = String.format(
                "{\"sub\":\"%s\",\"iss\":\"%s\",\"exp\":%d}",
                subject, issuer, expiry.getEpochSecond());
        return buildSignedJwtFromPayload(payload, kid, privateKey);
    }

    private static String buildSignedJwtFromPayload(String payload, String kid,
                                                     PrivateKey privateKey) throws Exception {
        String header = String.format("{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"%s\"}", kid);
        String encodedHeader = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(header.getBytes(StandardCharsets.UTF_8));
        String encodedPayload = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));

        String signingInput = encodedHeader + "." + encodedPayload;
        Signature sig = Signature.getInstance("SHA256withRSA");
        sig.initSign(privateKey);
        sig.update(signingInput.getBytes(StandardCharsets.UTF_8));
        byte[] signatureBytes = sig.sign();

        String encodedSignature = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(signatureBytes);
        return signingInput + "." + encodedSignature;
    }
}
