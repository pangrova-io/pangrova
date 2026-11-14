package org.pangrova.security.secret.resolution;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.common.exception.ConfigurationException;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.security.api.secret.SecretProvider;
import org.pangrova.security.api.secret.SecretProviderFactory;
import org.pangrova.security.api.secret.SecretReference;
import org.pangrova.security.api.secret.SecretResolver;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
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

    /** Creates an isolated registry for the installed providers of one process or plugin classloader. */
    public static SecretResolverRegistry discover(ClassLoader loader) {
        return install(ServiceLoader.load(SecretProviderFactory.class, loader));
    }

    static SecretResolverRegistry install(Iterable<SecretProviderFactory> installed) {
        var factories = new LinkedHashMap<String, SecretProviderFactory>();
        for (var factory : installed) {
            String prefix = requirePrefix(factory.prefix());
            if (factory.apiVersion() != SecretProviderFactory.API_VERSION) {
                throw new IllegalArgumentException("Incompatible secret provider API for prefix: " + prefix);
            }
            if (factories.putIfAbsent(prefix, factory) != null) {
                throw new IllegalArgumentException("Secret provider prefix already registered: " + prefix);
            }
        }
        var registry = new SecretResolverRegistry();
        factories.forEach((prefix, factory) -> {
            var provider = Objects.requireNonNull(factory.create(), "secret provider");
            if (!prefix.equals(provider.prefix())) {
                throw new IllegalArgumentException("Secret provider does not implement its declared prefix: " + prefix);
            }
            registry.register(provider);
        });
        return registry;
    }

    /**
     * Registers a secret provider for its prefix.
     *
     * @throws NullPointerException if provider or prefix is null
     * @throws IllegalArgumentException if the prefix is blank or already registered
     */
    public void register(SecretProvider provider) {
        Objects.requireNonNull(provider, "provider must not be null");
        String prefix = requirePrefix(provider.prefix());
        if (providers.putIfAbsent(prefix, provider) != null) {
            throw new IllegalArgumentException("Secret provider prefix already registered: " + prefix);
        }
    }

    private static String requirePrefix(String prefix) {
        Objects.requireNonNull(prefix, "provider prefix must not be null");
        if (prefix.isBlank()) throw new IllegalArgumentException("Secret provider prefix must not be blank");
        return prefix;
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
