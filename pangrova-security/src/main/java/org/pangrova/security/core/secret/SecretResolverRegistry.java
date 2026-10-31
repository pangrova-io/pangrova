package org.pangrova.security.core.secret;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.security.api.secret.SecretProvider;
import org.pangrova.security.api.secret.SecretReference;
import org.pangrova.security.api.secret.SecretResolver;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dispatches references to the {@link SecretProvider} registered for their prefix.
 *
 * <p>Concurrency strategy: ConcurrentHashMap for safe concurrent provider
 * registration and lookup.</p>
 */
@ThreadSafe
public final class SecretResolverRegistry implements SecretResolver {

    private final Map<String, SecretProvider> providers = new ConcurrentHashMap<>();

    /**
     * Registers a secret provider for its prefix.
     *
     * @throws NullPointerException if provider is null
     */
    public void register(SecretProvider provider) {
        Objects.requireNonNull(provider, "provider must not be null");
        providers.put(provider.prefix(), provider);
    }

    @Override
    public String resolve(SecretReference reference) {
        Objects.requireNonNull(reference, "reference must not be null");
        SecretProvider provider = providers.get(reference.prefix());
        if (provider == null) {
            throw new ConfigurationException(
                    PangrovaErrorCode.CONFIG_MISSING_KEY,
                    "No secret provider for prefix: " + reference.prefix());
        }
        return provider.resolve(reference);
    }
}
