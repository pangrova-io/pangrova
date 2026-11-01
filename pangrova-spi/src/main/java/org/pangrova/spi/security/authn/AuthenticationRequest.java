package org.pangrova.spi.security.authn;

import org.pangrova.common.api.annotation.Experimental;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Credentials submitted for authentication.
 *
 * <p>Password is stored as {@code char[]} rather than {@code String} to allow explicit
 * memory erasure after authentication completes. Strings are immutable and remain in the
 * heap until garbage collected, creating a window where credentials are exposed in memory.
 *
 * <p>Callers MUST call {@link #erase()} or use try-with-resources after authentication
 * to zero out the password array.</p>
 */
@Experimental
public final class AuthenticationRequest implements AutoCloseable {

    private final String username;
    private final char[] password;
    private final String token;
    private final String kerberosTicket;
    private final Map<String, String> extraCredentials;
    private final String clientIp;

    /**
     * Creates an authentication request.
     *
     * @param username submitted username
     * @param password submitted password
     * @param token bearer token
     * @param kerberosTicket Kerberos ticket
     * @param extraCredentials additional credentials
     * @param clientIp client network address
     */
    public AuthenticationRequest(
            String username,
            char[] password,
            String token,
            String kerberosTicket,
            Map<String, String> extraCredentials,
            String clientIp) {
        this.username = username;
        this.password = password != null ? password.clone() : null;
        this.token = token;
        this.kerberosTicket = kerberosTicket;
        this.extraCredentials = extraCredentials != null ? Map.copyOf(extraCredentials) : Map.of();
        this.clientIp = clientIp;
    }

    public String username() {
        return username;
    }

    /**
     * Returns a reference to the internal password array.
     * Do NOT store this reference — it will be zeroed after authentication.
     */
    public char[] password() {
        return password;
    }

    public String token() {
        return token;
    }

    public String kerberosTicket() {
        return kerberosTicket;
    }

    public Map<String, String> extraCredentials() {
        return extraCredentials;
    }

    public String clientIp() {
        return clientIp;
    }

    /**
     * Erases the password from memory by filling the char array with null characters.
     * This method is idempotent — safe to call multiple times.
     */
    public void erase() {
        if (password != null) {
            Arrays.fill(password, '\0');
        }
    }

    @Override
    public void close() {
        erase();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AuthenticationRequest that)) return false;
        return Objects.equals(username, that.username)
                && Arrays.equals(password, that.password)
                && Objects.equals(token, that.token)
                && Objects.equals(kerberosTicket, that.kerberosTicket)
                && Objects.equals(extraCredentials, that.extraCredentials)
                && Objects.equals(clientIp, that.clientIp);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(username, token, kerberosTicket, extraCredentials, clientIp);
        result = 31 * result + Arrays.hashCode(password);
        return result;
    }

    @Override
    public String toString() {
        return "AuthenticationRequest[username=" + username
                + ", password=***"
                + ", token=" + (token != null ? "***" : "null")
                + ", kerberosTicket=" + (kerberosTicket != null ? "***" : "null")
                + ", clientIp=" + clientIp + "]";
    }
}
