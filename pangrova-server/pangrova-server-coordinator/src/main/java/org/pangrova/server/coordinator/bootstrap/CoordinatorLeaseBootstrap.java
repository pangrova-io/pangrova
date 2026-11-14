package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.persistence.coordination.api.provider.CoordinationProvider;
import org.pangrova.persistence.coordination.api.provider.CoordinationProviderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;

/** Opens the selected membership and lease backend for the lifetime of this Coordinator. */
final class CoordinatorLeaseBootstrap {
    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorLeaseBootstrap.class);
    private static final String PREFIX = "pangrova.coordination.lease.";

    private CoordinatorLeaseBootstrap() {}

    static CoordinationProvider resolve(DataSource dataSource) {
        return open(dataSource, System.getProperty(PREFIX + "backend", "jdbc"),
                ServiceLoader.load(CoordinationProviderFactory.class, CoordinatorLeaseBootstrap.class.getClassLoader()));
    }

    static CoordinationProvider open(DataSource source, String backend, Iterable<CoordinationProviderFactory> factories) {
        Map<String, CoordinationProviderFactory> names = new HashMap<>();
        for (var factory : factories) {
            if (factory.apiVersion() != CoordinationProviderFactory.API_VERSION) {
                throw new IllegalStateException("Unsupported coordination provider API version: " + factory.id());
            }
            register(names, factory.id(), factory);
            for (String alias : factory.aliases()) {
                register(names, alias, factory);
            }
        }
        var chosen = names.get(backend.trim().toLowerCase(Locale.ROOT));
        if (chosen == null) {
            throw new IllegalArgumentException("Unknown coordination lease backend: " + backend
                    + ". Installed provider IDs and aliases: " + new java.util.TreeSet<>(names.keySet()));
        }
        var provider = Objects.requireNonNull(chosen.open(source,
                key -> System.getProperty(PREFIX + chosen.id() + "." + key)), "coordination provider");
        LOG.info("event=distributed_lease_configured backend={}", chosen.id());
        return provider;
    }

    private static void register(Map<String, CoordinationProviderFactory> names, String id,
                                 CoordinationProviderFactory factory) {
        if (id == null || !id.matches("[a-z][a-z0-9-]*") || names.putIfAbsent(id, factory) != null) {
            throw new IllegalArgumentException("Invalid or duplicate coordination provider ID: " + id);
        }
    }
}
