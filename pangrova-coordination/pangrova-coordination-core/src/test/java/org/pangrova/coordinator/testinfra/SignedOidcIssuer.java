package org.pangrova.coordinator.testinfra;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.net.InetSocketAddress;
import java.net.HttpURLConnection;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;

/** Actual signed OIDC issuer shared by this leaf's secured bootstrap tests. */
public final class SignedOidcIssuer implements AutoCloseable {
    private final Server server;
    private final KeyPair keys;

    /** Starts a real local discovery/JWKS issuer with a freshly generated RSA signing key. */
    public SignedOidcIssuer() throws Exception {
        var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        keys = generator.generateKeyPair();
        server = new Server(new InetSocketAddress("127.0.0.1", 0));
        server.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse response)
                    throws IOException {
                String json;
                if ("/.well-known/openid-configuration".equals(target)) {
                    json = "{\"jwks_uri\":\"" + uri() + "/jwks\"}";
                } else if ("/jwks".equals(target)) {
                    json = publicKeys();
                } else {
                    response.setStatus(HttpURLConnection.HTTP_NOT_FOUND);
                    base.setHandled(true);
                    return;
                }
                response.setStatus(HttpURLConnection.HTTP_OK);
                response.setContentType("application/json");
                response.getOutputStream().write(json.getBytes(StandardCharsets.UTF_8));
                base.setHandled(true);
            }
        });
        server.start();
    }

    private String publicKeys() {
        var key = (RSAPublicKey) keys.getPublic();
        return "{\"keys\":[{\"kty\":\"RSA\",\"kid\":\"bootstrap-key\",\"n\":\""
                + encode(unsigned(key.getModulus().toByteArray())) + "\",\"e\":\""
                + encode(unsigned(key.getPublicExponent().toByteArray())) + "\"}]}";
    }

    /** Returns the issuer's actual dynamically assigned endpoint. */
    public String uri() {
        return "http://127.0.0.1:" + server.getURI().getPort();
    }

    /** Creates a signed token, or an explicitly unsigned negative-test input. */
    public String token(String audience, boolean signed) throws Exception {
        String header = signed ? "{\"alg\":\"RS256\",\"kid\":\"bootstrap-key\"}" : "{\"alg\":\"none\"}";
        String payload = "{\"iss\":\"" + uri() + "\",\"sub\":\"alice\",\"aud\":\"" + audience
                + "\",\"groups\":[\"directory-analysts\"],\"exp\":" + Instant.now().plusSeconds(3600).getEpochSecond() + "}";
        String input = encode(header.getBytes(StandardCharsets.UTF_8)) + "."
                + encode(payload.getBytes(StandardCharsets.UTF_8));
        if (!signed) {
            return input + ".";
        }
        var signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keys.getPrivate());
        signature.update(input.getBytes(StandardCharsets.US_ASCII));
        return input + "." + encode(signature.sign());
    }

    private static byte[] unsigned(byte[] bytes) {
        return bytes[0] == 0 ? Arrays.copyOfRange(bytes, 1, bytes.length) : bytes;
    }

    private static String encode(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    @Override
    public void close() throws Exception {
        server.stop();
    }
}

