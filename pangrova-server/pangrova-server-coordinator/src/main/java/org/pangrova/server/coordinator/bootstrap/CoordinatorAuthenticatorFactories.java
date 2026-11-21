package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.security.api.authentication.AuthenticatorFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.ServiceLoader;

/** Discovers authenticator factories on the Coordinator classpath without starting any backend. */
final class CoordinatorAuthenticatorFactories {
    private CoordinatorAuthenticatorFactories() {}

    static AuthenticatorFactory select(String providerId) {
        return select(providerId, ServiceLoader.load(AuthenticatorFactory.class,
                CoordinatorAuthenticatorFactories.class.getClassLoader()));
    }

    static AuthenticatorFactory select(String providerId, Iterable<AuthenticatorFactory> factories) {
        var installed = new HashMap<String, AuthenticatorFactory>();
        for (var factory : factories) {
            String id = factory.providerId();
            if (id == null || id.isBlank() || !id.equals(id.strip().toUpperCase(Locale.ROOT))
                    || factory.apiVersion() != AuthenticatorFactory.API_VERSION) {
                throw new PangrovaException(PangrovaErrorCode.CONFIG_INVALID_VALUE,
                        "Invalid authenticator factory: " + id);
            }
            if (installed.putIfAbsent(id, factory) != null) {
                throw new PangrovaException(PangrovaErrorCode.CONFIG_INVALID_VALUE,
                        "Duplicate authenticator factory: " + id);
            }
        }
        var factory = providerId == null ? null : installed.get(providerId.toUpperCase(Locale.ROOT));
        if (factory == null) {
            throw new PangrovaException(PangrovaErrorCode.CONFIG_INVALID_VALUE,
                        "Unknown security.auth.type: " + providerId);
        }
        return factory;
    }
}
