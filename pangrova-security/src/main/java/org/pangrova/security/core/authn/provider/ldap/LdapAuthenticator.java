package org.pangrova.security.core.authn.provider.ldap;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.naming.AuthenticationException;
import javax.naming.ldap.Rdn;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.lifecycle.ResourceCloser;
import org.pangrova.spi.security.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.pangrova.spi.security.authn.AuthenticationRequest;
import org.pangrova.spi.security.authn.AuthenticationResult;
import org.pangrova.spi.security.authn.Authenticator;

/**
 * LDAP-based authenticator with real directory bind and group search.
 *
 * <p>Supports two authentication modes:
 * <ol>
 *   <li><b>Search-then-bind</b> (when {@code bindDn} is configured): binds with a service
 *       account first, searches for the user DN via {@code userSearchFilter} under
 *       {@code baseDn}, then re-binds as the found user to verify credentials.</li>
 *   <li><b>Direct bind</b> (when {@code bindDn} is absent): constructs the user DN from
 *       {@code userDnPattern} and binds directly.</li>
 * </ol>
 *
 * <p>After successful authentication, searches for group membership under
 * {@code groupSearchBase} using {@code groupSearchFilter}.</p>
 *
 * <p>Configuration keys:
 * <ul>
 *   <li>{@code ldapUrl} — LDAP server URL (e.g., ldap://localhost:389)</li>
 *   <li>{@code baseDn} — Base DN for user search (search-then-bind mode)</li>
 *   <li>{@code userSearchFilter} — Filter template for user lookup, e.g. (uid={0})</li>
 *   <li>{@code userDnPattern} — DN pattern with {0} placeholder (direct bind mode)</li>
 *   <li>{@code bindDn} — Service account DN for initial bind (optional)</li>
 *   <li>{@code bindPassword} — Service account password (optional)</li>
 *   <li>{@code groupSearchBase} — Base DN for group search</li>
 *   <li>{@code groupSearchFilter} — Filter for group membership lookup</li>
 *   <li>{@code groupAttribute} — Attribute name for group name (default: cn)</li>
 *   <li>{@code connectTimeoutMs} — Connection timeout in ms (default: 5000)</li>
 *   <li>{@code readTimeoutMs} — Read timeout in ms (default: 5000)</li>
 * </ul>
 *
 */
@ThreadSafe
public class LdapAuthenticator implements Authenticator {

    private static final Logger LOG = LoggerFactory.getLogger(LdapAuthenticator.class);

    /**
     * Immutable LDAP configuration snapshot.
     *
     * <p>{@code bindPassword} is stored as {@code char[]} to avoid lingering
     * in the JVM string pool. Callers should call {@link #clearBindPassword()}
     * when the config is no longer needed (e.g., during config rotation).</p>
     */
    record LdapConfig(
            String ldapUrl,
            String baseDn,
            String userSearchFilter,
            String userDnPattern,
            String bindDn,
            char[] bindPassword,
            String groupSearchBase,
            String groupSearchFilter,
            String groupAttribute,
            String connectTimeoutMs,
            String readTimeoutMs
    ) {
        /** True when a service account is configured for search-then-bind. */
        boolean isSearchThenBind() {
            return bindDn != null && !bindDn.isBlank();
        }

        /** Zeroes out the bind password array to prevent memory-resident credential exposure. */
        void clearBindPassword() {
            if (bindPassword != null) {
                Arrays.fill(bindPassword, '\0');
            }
        }
    }

    private final AtomicReference<LdapConfig> configRef = new AtomicReference<>();

    @Override
    public String getName() {
        return "ldap";
    }

    @Override
    public void init(Map<String, String> config) {
        String rawPassword = config.get("bindPassword");
        char[] passwordChars = rawPassword != null ? rawPassword.toCharArray() : null;
        LdapConfig ldapConfig = new LdapConfig(
                config.getOrDefault("ldapUrl", "ldaps://localhost:636"),
                config.getOrDefault("baseDn", "ou=users,dc=example,dc=com"),
                config.getOrDefault("userSearchFilter", "(uid={0})"),
                config.getOrDefault("userDnPattern", "uid={0},ou=users,dc=example,dc=com"),
                config.get("bindDn"),
                passwordChars,
                config.getOrDefault("groupSearchBase", "ou=groups,dc=example,dc=com"),
                config.getOrDefault("groupSearchFilter", "(member={0})"),
                config.getOrDefault("groupAttribute", "cn"),
                config.getOrDefault("connectTimeoutMs", "5000"),
                config.getOrDefault("readTimeoutMs", "5000")
        );
        LdapConfig oldConfig = configRef.getAndSet(ldapConfig);
        if (oldConfig != null) {
            oldConfig.clearBindPassword();
        }
        LOG.info("event=ldap_init ldapUrl={} mode={} baseDn={} groupSearchBase={}",
                ldapConfig.ldapUrl(),
                ldapConfig.isSearchThenBind() ? "search-then-bind" : "direct-bind",
                ldapConfig.baseDn(),
                ldapConfig.groupSearchBase());
    }

    @Override
    public AuthenticationResult authenticate(AuthenticationRequest request) {
        if (request.username() == null || request.password() == null) {
            return AuthenticationResult.failure("Username and password required for LDAP");
        }

        LdapConfig cfg = configRef.get();
        if (cfg == null) {
            LOG.warn("event=ldap_auth_failed reason=not_initialized clientIp={}", request.clientIp());
            return AuthenticationResult.failure("LDAP not initialized");
        }

        return performLdapBind(cfg, request);
    }

    private AuthenticationResult performLdapBind(LdapConfig cfg, AuthenticationRequest request) {
        try {
            String userDn = resolveUserDn(cfg, request.username());
            return bindAndResolveIdentity(cfg, request, userDn);
        } catch (AuthenticationException e) {
            LOG.warn("event=ldap_auth_failed reason=invalid_credentials username={} clientIp={}",
                    request.username(), request.clientIp());
            return AuthenticationResult.failure("Invalid credentials");
        } catch (NamingException e) {
            LOG.error("event=ldap_auth_error username={} clientIp={} error={}",
                    request.username(), request.clientIp(), e.getMessage(), e);
            return AuthenticationResult.failure("LDAP authentication error");
        }
    }

    private AuthenticationResult bindAndResolveIdentity(
            LdapConfig cfg, AuthenticationRequest request, String userDn) throws NamingException {
        DirContext userCtx = null;
        try {
            userCtx = createBindContext(cfg, userDn, request.password());
            Set<String> groups = searchGroupsSafe(userCtx, cfg, userDn);
            UserIdentity identity = new UserIdentity(
                    request.username(), groups, Map.of(), Instant.now());
            LOG.info("event=ldap_auth_success principal={} groups={} clientIp={}",
                    request.username(), groups.size(), request.clientIp());
            return AuthenticationResult.success(identity);
        } finally {
            if (userCtx != null) {
                ResourceCloser.closeQuietly(userCtx::close);
            }
        }
    }

    private Set<String> searchGroupsSafe(DirContext ctx, LdapConfig cfg, String userDn) {
        try {
            return searchGroups(ctx, cfg, userDn);
        } catch (NamingException e) {
            LOG.debug("event=ldap_group_search_failed userDn={} base={} error={}",
                    userDn, cfg.groupSearchBase(), e.getMessage());
            return Set.of();
        }
    }

    @Override
    public boolean supports(AuthenticationRequest request) {
        return request.username() != null
                && request.password() != null
                && request.token() == null;
    }

    @Override
    public boolean isHealthy() {
        LdapConfig cfg = configRef.get();
        if (cfg == null) {
            return false;
        }
        return tryHealthCheck(cfg);
    }

    private boolean tryHealthCheck(LdapConfig cfg) {
        DirContext ctx = null;
        try {
            ctx = openHealthCheckContext(cfg);
            return true;
        } catch (NamingException e) {
            LOG.debug("event=ldap_health_check_failed ldapUrl={} error={}", cfg.ldapUrl(), e.getMessage());
            return false;
        } finally {
            if (ctx != null) {
                ResourceCloser.closeQuietly(ctx::close);
            }
        }
    }

    private DirContext openHealthCheckContext(LdapConfig cfg) throws NamingException {
        if (cfg.isSearchThenBind()) {
            return createBindContext(cfg, cfg.bindDn(), cfg.bindPassword());
        }
        Hashtable<String, String> env = buildBaseEnvironment(cfg);
        return new InitialDirContext(env);
    }

    @Override
    public void close() {
        LdapConfig cfg = configRef.getAndSet(null);
        if (cfg != null) {
            cfg.clearBindPassword();
        }
        LOG.debug("event=ldap_close");
    }

    // --- Internal LDAP operations ---

    /**
     * Resolves the user DN either via search-then-bind or direct pattern substitution.
     */
    String resolveUserDn(LdapConfig cfg, String username) throws NamingException {
        if (cfg.isSearchThenBind()) {
            return searchUserDn(cfg, username);
        }
        return cfg.userDnPattern().replace("{0}", Rdn.escapeValue(username));
    }

    /**
     * Binds with the service account and searches for the user DN.
     *
     * @throws NamingException if the service account bind or user search fails
     * @throws AuthenticationException if the user is not found in the directory
     */
    private String searchUserDn(LdapConfig cfg, String username) throws NamingException {
        DirContext serviceCtx = null;
        try {
            serviceCtx = createBindContext(cfg, cfg.bindDn(), cfg.bindPassword());
            return executeUserSearch(serviceCtx, cfg, username);
        } finally {
            if (serviceCtx != null) {
                ResourceCloser.closeQuietly(serviceCtx::close);
            }
        }
    }

    private String executeUserSearch(
            DirContext ctx, LdapConfig cfg, String username) throws NamingException {
        SearchControls controls = new SearchControls();
        controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        controls.setReturningAttributes(new String[]{"dn"});
        controls.setCountLimit(1);

        String filter = cfg.userSearchFilter().replace("{0}", escapeFilterValue(username));
        NamingEnumeration<SearchResult> results = ctx.search(cfg.baseDn(), filter, controls);
        try {
            if (!results.hasMore()) {
                throw new AuthenticationException("User not found in directory");
            }
            String dn = results.next().getNameInNamespace();
            LOG.debug("event=ldap_user_found username={} dn={}", username, dn);
            return dn;
        } finally {
            results.close();
        }
    }

    DirContext createBindContext(LdapConfig cfg, String dn, char[] password) throws NamingException {
        Hashtable<String, Object> env = new Hashtable<>(buildBaseEnvironment(cfg));
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, dn);
        // Pass as byte[] to avoid lingering credentials in the String pool.
        // JNDI LDAP provider accepts byte[] for SECURITY_CREDENTIALS.
        byte[] credBytes = toUtf8Bytes(password);
        env.put(Context.SECURITY_CREDENTIALS, credBytes);
        try {
            return new InitialDirContext(env);
        } finally {
            Arrays.fill(credBytes, (byte) 0);
        }
    }

    /**
     * Converts a char[] password to UTF-8 bytes without creating an intermediate String,
     * preventing credential exposure in the JVM String pool.
     */
    private static byte[] toUtf8Bytes(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(charBuffer);
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        // Clear the encoder's internal buffer
        if (byteBuffer.hasArray()) {
            Arrays.fill(byteBuffer.array(), (byte) 0);
        }
        return bytes;
    }

    private Hashtable<String, String> buildBaseEnvironment(LdapConfig cfg) {
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, cfg.ldapUrl());
        env.put("com.sun.jndi.ldap.connect.timeout", cfg.connectTimeoutMs());
        env.put("com.sun.jndi.ldap.read.timeout", cfg.readTimeoutMs());
        env.put(Context.REFERRAL, "ignore");
        return env;
    }

    Set<String> searchGroups(DirContext ctx, LdapConfig cfg, String userDn) throws NamingException {
        SearchControls controls = new SearchControls();
        controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        controls.setReturningAttributes(new String[]{cfg.groupAttribute()});

        String filter = cfg.groupSearchFilter().replace("{0}", escapeFilterValue(userDn));
        NamingEnumeration<SearchResult> results = ctx.search(cfg.groupSearchBase(), filter, controls);

        Set<String> groups = new HashSet<>();
        try {
            while (results.hasMore()) {
                collectGroupAttributes(results.next(), cfg.groupAttribute(), groups);
            }
        } finally {
            results.close();
        }
        return Set.copyOf(groups);
    }

    private static void collectGroupAttributes(
            SearchResult sr, String groupAttribute, Set<String> groups) throws NamingException {
        Attribute attr = sr.getAttributes().get(groupAttribute);
        if (attr == null) {
            return;
        }
        for (int i = 0; i < attr.size(); i++) {
            groups.add(attr.get(i).toString());
        }
    }

    /**
     * Escapes special characters in LDAP filter values per RFC 4515.
     */
    static String escapeFilterValue(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(value.length());
        for (char c : value.toCharArray()) {
            switch (c) {
                case '\\' -> sb.append("\\5c");
                case '*' -> sb.append("\\2a");
                case '(' -> sb.append("\\28");
                case ')' -> sb.append("\\29");
                case '\0' -> sb.append("\\00");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

}
