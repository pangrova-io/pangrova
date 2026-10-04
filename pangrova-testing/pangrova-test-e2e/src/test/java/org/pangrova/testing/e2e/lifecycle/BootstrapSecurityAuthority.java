package org.pangrova.testing.e2e.lifecycle;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;
import org.pangrova.testing.e2e.SharedTestContainers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

/** Real PostgreSQL schema and RSA/JWKS discovery authority; no auth bypass or fake Coordinator. */
final class BootstrapSecurityAuthority implements AutoCloseable {
    private final String schema = "worker_bootstrap_" + UUID.randomUUID().toString().replace("-", "");
    private final String secret = "bootstrap-" + UUID.randomUUID();
    private final Server issuer;
    private final String publicKeys;

    BootstrapSecurityAuthority() throws Exception {
        if (!SharedTestContainers.AVAILABLE || SharedTestContainers.POSTGRES == null) {
            throw new IllegalStateException("Worker bootstrap requires actual PostgreSQL authority");
        }
        var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        var key = (RSAPublicKey) generator.generateKeyPair().getPublic();
        publicKeys = "{\"keys\":[{\"kty\":\"RSA\",\"kid\":\"bootstrap-key\",\"n\":\""
                + unsigned(key.getModulus().toByteArray()) + "\",\"e\":\""
                + unsigned(key.getPublicExponent().toByteArray()) + "\"}]}";
        issuer = new Server(new InetSocketAddress("127.0.0.1", 0));
        issuer.setHandler(new DiscoveryHandler());
        issuer.start();
        try {
            executeSchemaSql("CREATE SCHEMA " + schema);
        } catch (Exception failure) {
            try { issuer.stop(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    CoordinatorBootstrapConfig config(String id) {
        var defaults = CoordinatorBootstrapConfig.withDefaults(id, 0);
        var pg = SharedTestContainers.POSTGRES;
        return new CoordinatorBootstrapConfig(defaults.core(),
                new CoordinatorBootstrapConfig.FeatureFlags(false, true, true, false, false, false, false, false),
                defaults.query(), defaults.transport(), defaults.workerHealth(),
                new CoordinatorBootstrapConfig.MetadataJdbcConfig(pg.getJdbcUrl()
                        + (pg.getJdbcUrl().contains("?") ? "&" : "?") + "currentSchema=" + schema,
                        pg.getUsername(), pg.getPassword()),
                new CoordinatorBootstrapConfig.SecurityConfig(
                        CoordinatorBootstrapConfig.SecurityConfig.AuthenticatorType.OAUTH2, secret,
                        Map.of("issuerUri", issuerUri(), "audience", "worker-bootstrap")), null);
    }

    String secret() { return secret; }
    private String issuerUri() { return "http://127.0.0.1:" + issuer.getURI().getPort(); }

    private void executeSchemaSql(String sql) throws Exception {
        var pg = SharedTestContainers.POSTGRES;
        try (var connection = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
             var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String unsigned(byte[] value) {
        byte[] bytes = value[0] == 0 ? Arrays.copyOfRange(value, 1, value.length) : value;
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        try { issuer.stop(); } catch (Exception cleanup) { failure = cleanup; }
        try { executeSchemaSql("DROP SCHEMA " + schema + " CASCADE"); }
        catch (Exception cleanup) {
            if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
        }
        if (failure != null) throw failure;
    }

    private final class DiscoveryHandler extends AbstractHandler {
        @Override
        public void handle(String target, Request base, HttpServletRequest request, HttpServletResponse response)
                throws IOException {
            String json;
            if (target.equals("/.well-known/openid-configuration")) {
                json = "{\"jwks_uri\":\"" + issuerUri() + "/jwks\"}";
            } else if (target.equals("/jwks")) {
                json = publicKeys;
            } else {
                response.setStatus(404);
                base.setHandled(true);
                return;
            }
            response.setStatus(200);
            response.setContentType("application/json");
            response.getOutputStream().write(json.getBytes(StandardCharsets.UTF_8));
            base.setHandled(true);
        }
    }
}
