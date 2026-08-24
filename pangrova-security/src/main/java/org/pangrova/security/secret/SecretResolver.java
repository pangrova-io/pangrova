package org.pangrova.security.secret;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.ConfigurationException;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses ${PREFIX:reference} syntax and routes to the matching {@link SecretProvider}.
 *
 * <p>Concurrency strategy: ConcurrentHashMap for safe concurrent provider registration and lookup.</p>
 */
@ThreadSafe
public class SecretResolver {

    private static final Pattern SECRET_PATTERN = Pattern.compile("\\$\\{([A-Z]+):(.+?)\\}");

    private final Map<String, SecretProvider> providers = new ConcurrentHashMap<>();

    /**
     * Registers a secret provider for its prefix.
     *
     * @param provider the secret provider to register
     * @throws NullPointerException if provider is null
     */
    public void register(SecretProvider provider) {
        Objects.requireNonNull(provider, "provider must not be null");
        providers.put(provider.prefix(), provider);
    }

    /**
     * Resolves secret references in the given value string.
     *
     * @param value the value potentially containing ${PREFIX:reference} syntax
     * @return the resolved value, or the original value if no secret reference found
     */
    public String resolve(String value) {
        if (value == null || !value.contains("${")) {
            return value;
        }
        Matcher matcher = SECRET_PATTERN.matcher(value);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String resolved = resolveReference(matcher.group(1), matcher.group(2));
            matcher.appendReplacement(sb, Matcher.quoteReplacement(resolved));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String resolveReference(String prefix, String reference) {
        SecretProvider provider = providers.get(prefix);
        if (provider == null) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_MISSING_KEY,
                    "No secret provider for prefix: " + prefix);
        }
        return provider.resolve(reference);
    }
}
