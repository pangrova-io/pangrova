package org.pangrova.worker.catalog;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.connector.api.metadata.ConnectorOutputHandleCodec;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.plugin.ConnectorContext;
import org.pangrova.connector.api.plugin.ConnectorFactory;
import org.pangrova.connector.runtime.lifecycle.ConnectorPluginInventory;
import org.pangrova.protocol.catalog.CatalogDeploymentFingerprint;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.security.api.secret.SecretResolver;
import org.pangrova.security.secret.resolution.SecretTemplates;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Owns the tenant-scoped connector cache, counted leases and retriable connector shutdown. */
@ThreadSafe
final class WorkerCatalogRegistry implements AutoCloseable {
    private static final int MAX_CACHED_CATALOG_REVISIONS = 1024;

    private final CatalogDeploymentClient deployments;
    private final ConnectorPluginInventory plugins;
    private final SecretResolver secrets;
    private final Object catalogLock = new Object();

    /** Access-ordered cache; all accesses are guarded by catalogLock. */
    private final LinkedHashMap<CatalogRevisionKey, CatalogEntry> catalogs =
            new LinkedHashMap<>(16, 0.75f, true);

    /** Eviction transfers ownership here until shutdown actually completes. */
    private final java.util.Set<CatalogEntry> retiringCatalogs =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

    private boolean closed;

    /** Creates a runtime from trusted deployment and plugin inventories. */
    WorkerCatalogRegistry(
            CatalogDeploymentClient deployments,
            ConnectorPluginInventory plugins,
            SecretResolver secrets) {
        this.deployments = java.util.Objects.requireNonNull(deployments, "deployments");
        this.plugins = plugins;
        this.secrets = java.util.Objects.requireNonNull(secrets, "secrets");
        if (plugins.registry().names().isEmpty()) {
            throw new IllegalStateException("No Worker connector plugins are installed");
        }
    }

    /** Publishes a validated binding only while this cache still accepts work. */
    void publish(Runnable publishBinding) {
        synchronized (catalogLock) {
            requireOpen();
            publishBinding.run();
        }
    }

    @Override
    public void close() {
        List<CatalogEntry> entries;
        synchronized (catalogLock) {
            closed = true;
            var owned = new java.util.ArrayList<>(catalogs.values());
            owned.addAll(retiringCatalogs);
            entries = List.copyOf(owned);
        }
        Throwable failure = null;
        for (CatalogEntry entry : entries) {
            try {
                synchronized (catalogLock) {
                    if (entry.leases != 0) {
                        throw new IllegalStateException("Worker catalog still has active leases");
                    }
                }
                entry.shutdown.shutdown();
                removeClosedEntry(entry);
            } catch (RuntimeException | Error shutdownFailure) {
                if (failure == null) {
                    failure = shutdownFailure;
                } else if (failure != shutdownFailure) {
                    failure.addSuppressed(shutdownFailure);
                }
            }
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
    }

    private void removeClosedEntry(CatalogEntry entry) {
        synchronized (catalogLock) {
            catalogs.values().removeIf(value -> value == entry);
            retiringCatalogs.remove(entry);
        }
    }

    int size() {
        synchronized (catalogLock) {
            return catalogs.size();
        }
    }

    CatalogEntry acquireCatalog(CatalogExecutionBinding binding, String tenantId) {
        CatalogRevisionKey key = CatalogRevisionKey.from(binding, tenantId);
        CatalogEntry entry;
        CatalogEntry evicted = null;
        boolean created = false;
        synchronized (catalogLock) {
            requireOpen();
            entry = catalogs.get(key);
            if (entry == null) {
                if (!retiringCatalogs.isEmpty()) {
                    throw new IllegalStateException(
                            "Worker catalog shutdown requires retry before new admission");
                }
                if (catalogs.size() >= MAX_CACHED_CATALOG_REVISIONS) {
                    evicted = evictIdleCatalog();
                }
                entry = new CatalogEntry();
                catalogs.put(key, entry);
                created = true;
            }
            entry.leases++;
        }
        if (created) {
            initializeCatalog(key, entry, evicted, binding, tenantId);
        }
        return entry;
    }

    /** Removes the least-recently-used unleased entry without closing it under the lock. */
    private CatalogEntry evictIdleCatalog() {
        var candidates = catalogs.entrySet().iterator();
        while (candidates.hasNext()) {
            CatalogEntry entry = candidates.next().getValue();
            if (entry.leases == 0 && entry.ready.isDone()) {
                retiringCatalogs.add(entry);
                candidates.remove();
                return entry;
            }
        }
        throw new IllegalStateException("Worker catalog cache is full of active task leases");
    }

    private void initializeCatalog(
            CatalogRevisionKey key,
            CatalogEntry entry,
            CatalogEntry evicted,
            CatalogExecutionBinding binding,
            String tenantId) {
        try {
            if (evicted != null) {
                evicted.shutdown.shutdown();
                removeClosedEntry(evicted);
            }
            CatalogRuntime runtime = loadCatalog(entry, binding, tenantId);
            entry.shutdown.initializationComplete();
            entry.ready.complete(runtime);
        } catch (RuntimeException | Error failure) {
            entry.shutdown.initializationComplete();
            try {
                entry.shutdown.shutdown();
                synchronized (catalogLock) {
                    catalogs.remove(key, entry);
                }
            } catch (RuntimeException | Error cleanup) {
                if (failure != cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            entry.ready.completeExceptionally(failure);
        }
    }

    static CatalogRuntime awaitCatalog(CatalogEntry entry) {
        try {
            return entry.ready.join();
        } catch (CompletionException failed) {
            if (failed.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failed.getCause() instanceof Error error) {
                throw error;
            }
            throw failed;
        }
    }

    void releaseCatalog(CatalogEntry entry) {
        synchronized (catalogLock) {
            entry.leases--;
        }
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("Worker catalog runtime is closed");
        }
    }

    private CatalogRuntime loadCatalog(
            CatalogEntry entry, CatalogExecutionBinding binding, String tenantId) {
        CatalogDeploymentResponse deployment =
                deployments.fetch(
                        binding.getCatalogName(),
                        binding.getCatalogIncarnationId(),
                        binding.getCatalogRevision());
        requireDeploymentIdentity(binding, deployment);
        validateTenant(deployment, tenantId);
        ConnectorFactory factory = plugins.registry().find(deployment.connectorProvider()).orElse(null);
        if (factory == null) {
            throw new IllegalStateException(
                    "Connector plugin is not installed: " + deployment.connectorProvider());
        }
        Map<String, String> resolved = resolveSecrets(deployment.properties(), tenantId);
        factory.validateConfiguration(resolved);
        Connector connector =
                factory.create(deployment.catalogName(), resolved, new WorkerConnectorContext());
        entry.shutdown.attach(connector);
        ConnectorOutputHandleCodec codec = connector.getOutputHandleCodec();
        String provider = deployment.connectorProvider();
        boolean nativeStorage = java.util.Set.of("lance", "paimon").contains(provider)
                || ("iceberg".equals(provider) && resolved.getOrDefault("warehouse", "").startsWith("s3://"));
        var storage = nativeStorage ? NativeObjectStoreBindings.create(binding, tenantId, resolved) : null;
        return new CatalogRuntime(connector, codec, storage);
    }

    private Map<String, String> resolveSecrets(Map<String, String> properties, String tenantId) {
        Map<String, String> resolved = new LinkedHashMap<>();
        properties.forEach((key, value) -> resolved.put(key, SecretTemplates.resolve(secrets, value, tenantId)));
        return Map.copyOf(resolved);
    }

    private static void requireDeploymentIdentity(
            CatalogExecutionBinding expected, CatalogDeploymentResponse actual) {
        if (!expected.getCatalogName().equals(actual.catalogName())
                || !expected.getConnectorProvider().equals(actual.connectorProvider())
                || !expected.getCatalogIncarnationId().equals(actual.catalogIncarnationId())
                || expected.getCatalogRevision() != actual.catalogRevision()
                || !CatalogDeploymentFingerprint.matches(
                        expected.getConfigurationFingerprint(), actual)) {
            throw new IllegalStateException("Catalog deployment identity/fingerprint mismatch");
        }
    }

    private static void validateTenant(CatalogDeploymentResponse deployment, String tenantId) {
        if (!deployment.allowedTenants().isEmpty()
                && !deployment.allowedTenants().contains(tenantId)) {
            throw new SecurityException("Tenant is not authorized for catalog deployment");
        }
    }

    record CatalogRuntime(
            Connector connector,
            ConnectorOutputHandleCodec codec,
            org.pangrova.protocol.v2.ObjectStoreExecutionBinding storage) {}

    static final class CatalogEntry {
        private final CompletableFuture<CatalogRuntime> ready = new CompletableFuture<>();
        private final CatalogShutdownOwner shutdown = new CatalogShutdownOwner();
        private int leases;
    }

    private static final class WorkerConnectorContext implements ConnectorContext {
        @Override
        public java.util.Optional<org.pangrova.connector.api.metadata.NativeDatasetControlPlane>
                nativeDatasetControlPlane() {
            return java.util.Optional.empty();
        }
    }
}
