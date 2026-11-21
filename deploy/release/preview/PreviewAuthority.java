import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.pangrova.persistence.security.postgresql.policy.JdbcGrantStore;
import org.pangrova.persistence.security.postgresql.policy.JdbcRoleStore;
import org.pangrova.persistence.security.postgresql.tenant.JdbcTenantStore;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.enums.TenantRole;
import org.pangrova.security.api.enums.TenantStatus;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.api.policy.Role;
import org.pangrova.security.api.policy.UserRoleAssignment;
import org.pangrova.security.api.tenant.ResourceQuota;
import org.pangrova.security.api.tenant.Tenant;
import org.postgresql.ds.PGSimpleDataSource;

/** Owns the disposable preview's issuer and grants in the preview database. */
public class PreviewAuthority {
    private static final String PRINCIPAL = "iceberg_preview";

    public static void main(String[] args) throws Exception {
        var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        var key = generator.generateKeyPair();
        var publicKey = (RSAPublicKey) key.getPublic();
        var issuer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String uri = "http://127.0.0.1:" + issuer.getAddress().getPort();
        route(issuer, "/.well-known/openid-configuration",
                "{\"issuer\":\"" + uri + "\",\"jwks_uri\":\"" + uri + "/jwks\"}");
        route(issuer, "/jwks", "{\"keys\":[{\"kty\":\"RSA\",\"alg\":\"RS256\",\"kid\":\"preview\",\"n\":\""
                + number(publicKey.getModulus()) + "\",\"e\":\"" + number(publicKey.getPublicExponent()) + "\"}]}");
        issuer.start();
        try {
            Path directory = Path.of(args[0]);
            Path issuerFile = Files.writeString(directory.resolve("issuer.tmp"), uri);
            Files.move(issuerFile, directory.resolve("issuer"));
            if (System.in.read() != 'G') {
                return;
            }
            provision();
            long now = Instant.now().getEpochSecond();
            String payload = "{\"iss\":\"" + uri + "\",\"aud\":\"pangrova-preview\",\"sub\":\""
                    + PRINCIPAL + "\",\"iat\":" + now + ",\"exp\":" + (now + 600) + "}";
            String input = encode("{\"kid\":\"preview\",\"alg\":\"RS256\"}".getBytes(StandardCharsets.UTF_8))
                    + "." + encode(payload.getBytes(StandardCharsets.UTF_8));
            var signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(key.getPrivate());
            signature.update(input.getBytes(StandardCharsets.UTF_8));
            Path token = Files.createFile(directory.resolve("token.tmp"),
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.writeString(token, input + "." + encode(signature.sign()));
            Files.move(token, directory.resolve("token"));
            System.in.read();
        } finally {
            issuer.stop(0);
        }
    }

    private static void provision() {
        var source = new PGSimpleDataSource();
        source.setURL("jdbc:postgresql://metadata:5432/pangrova_meta");
        source.setUser("pangrova");
        source.setPassword(System.getenv("PANGROVA_PREVIEW_DB_PASSWORD"));

        var quota = new ResourceQuota(1_073_741_824L, 10, 10, 60_000, 1_073_741_824L,
                1000, 1_099_511_627_776L, 1_099_511_627_776L);
        var tenants = new JdbcTenantStore(source);
        tenants.createTenant(new Tenant(PRINCIPAL, "Iceberg preview", TenantStatus.ACTIVE,
                quota, List.of(), List.of(), null, Map.of(), Instant.EPOCH, PRINCIPAL));
        tenants.addUserToTenant(PRINCIPAL, PRINCIPAL, TenantRole.OWNER);

        var roles = new JdbcRoleStore(source);
        roles.createRole(Role.of(PRINCIPAL));
        roles.grantRoleToUser(UserRoleAssignment.of(PRINCIPAL, PRINCIPAL));
        new JdbcGrantStore(source).addGrant(
                PrivilegeGrant.of(PRINCIPAL, Privilege.OWNER, "CATALOG", "*", null, null, null));
    }

    private static void route(HttpServer server, String path, String json) {
        byte[] response = json.getBytes(StandardCharsets.UTF_8);
        server.createContext(path, exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            try (exchange) {
                exchange.sendResponseHeaders(200, response.length);
                try (var output = exchange.getResponseBody()) {
                    output.write(response);
                }
            }
        });
    }

    private static String encode(byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    private static String number(BigInteger value) {
        byte[] bytes = value.toByteArray();
        return encode(bytes[0] == 0 ? Arrays.copyOfRange(bytes, 1, bytes.length) : bytes);
    }
}
