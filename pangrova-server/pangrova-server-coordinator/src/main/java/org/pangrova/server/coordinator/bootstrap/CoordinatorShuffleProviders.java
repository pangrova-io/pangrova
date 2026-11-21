package org.pangrova.server.coordinator.bootstrap;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

import org.pangrova.config.enums.ShuffleMode;
import org.pangrova.shuffle.api.ShuffleExecutionOwnerResolver;
import org.pangrova.shuffle.api.coordinator.ShuffleCoordinatorFactory;
import org.pangrova.shuffle.api.coordinator.ShuffleCoordinatorSession;
import org.pangrova.shuffle.api.enums.ShuffleCapability;

/** Discovers the configured external control provider before opening any backend resources. */
final class CoordinatorShuffleProviders {
    private CoordinatorShuffleProviders() {}

    static void validate(CoordinatorBootstrapConfig config) {
        if (config.shuffleMode() == ShuffleMode.LOCAL && config.speculationEnabled()) {
            throw new IllegalArgumentException("Local shuffle does not support speculative execution");
        }
        if (config.shuffleMode() == ShuffleMode.REMOTE) {
            var factory = configuredFactory(config, requiredCapabilities(config));
            factory.validate(settings(config));
        }
    }

    static ShuffleCoordinatorSession open(CoordinatorBootstrapConfig config, ShuffleExecutionOwnerResolver owners) {
        if (config.shuffleMode() != ShuffleMode.REMOTE) {
            validate(config);
            return null;
        }
        var required = requiredCapabilities(config);
        var factory = configuredFactory(config, required);
        var session = Objects.requireNonNull(factory.open(settings(config), owners), "shuffle session");
        try {
            var actual = session.capabilities().capabilities();
            if (!actual.containsAll(required) || !factory.capabilities().capabilities().containsAll(actual)) {
                throw new IllegalStateException(
                        "Shuffle provider session does not satisfy the configured capabilities");
            }
            return session;
        } catch (RuntimeException | Error failure) {
            try {
                session.close();
            } catch (RuntimeException | Error cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static ShuffleCoordinatorFactory configuredFactory(CoordinatorBootstrapConfig config,
            Set<ShuffleCapability> required) {
        return select(config.featureFlags().shuffleProvider(), required,
                ServiceLoader.load(ShuffleCoordinatorFactory.class,
                        CoordinatorShuffleProviders.class.getClassLoader()));
    }

    private static Set<ShuffleCapability> requiredCapabilities(CoordinatorBootstrapConfig config) {
        var required = EnumSet.of(ShuffleCapability.BLOCKING_READ, ShuffleCapability.PARTITION_READ);
        if (config.speculationEnabled()) {
            required.add(ShuffleCapability.SPECULATIVE_ATTEMPT_ISOLATION);
        }
        return required;
    }

    private static Map<String, String> settings(CoordinatorBootstrapConfig config) {
        Map<String, String> settings = new HashMap<>(config.featureFlags().shuffleProviderSettings());
        if (!config.shuffleEndpoints().isEmpty()) {
            add(settings, "endpoints", String.join(",", config.shuffleEndpoints()));
        }
        var tls = config.shuffleTlsConfig();
        if (tls != null) {
            add(settings, "tls.server-identity", tls.serverIdentity());
            add(settings, "tls.certificate-chain", tls.certificateChain().toString());
            add(settings, "tls.private-key", tls.privateKey().toString());
            add(settings, "tls.root-ca", tls.rootCa().toString());
        }
        return Map.copyOf(settings);
    }

    private static void add(Map<String, String> settings, String key, String value) {
        if (settings.putIfAbsent(key, value) != null) {
            throw new IllegalArgumentException("Duplicate shuffle provider configuration: " + key);
        }
    }

    static ShuffleCoordinatorFactory select(String selected, Set<ShuffleCapability> required,
            Iterable<ShuffleCoordinatorFactory> factories) {
        Map<String, ShuffleCoordinatorFactory> installed = new HashMap<>();
        for (var factory : factories) {
            String id = factory.providerId();
            if (id == null || !id.matches("[a-z][a-z0-9-]*")
                    || factory.apiVersion() != ShuffleCoordinatorFactory.API_VERSION) {
                throw new IllegalArgumentException("Invalid shuffle coordinator factory: " + id);
            }
            if (installed.putIfAbsent(id, factory) != null) {
                throw new IllegalArgumentException("Duplicate shuffle provider: " + id);
            }
        }
        var factory = installed.get(selected);
        if (factory == null) {
            throw new IllegalArgumentException("Unknown shuffle coordinator provider: " + selected);
        }
        if (!factory.capabilities().capabilities().containsAll(required)) {
            throw new IllegalArgumentException("Shuffle provider " + selected + " lacks required capabilities "
                    + required);
        }
        return factory;
    }
}
