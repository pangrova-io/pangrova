package org.pangrova.worker.jni.write.catalog;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.worker.jni.write.TaskConnectorSession;
import org.pangrova.worker.jni.write.secret.EnvironmentConnectorSecretResolver;

import org.pangrova.protocol.CatalogDeploymentFingerprint;
import org.pangrova.protocol.rest.catalog.CatalogDeploymentResponse;
import org.pangrova.protocol.v2.CatalogExecutionBinding;
import org.pangrova.protocol.v2.SerializedConnectorOutputHandle;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorContext;
import org.pangrova.spi.connector.ConnectorFactory;
import org.pangrova.spi.connector.ConnectorSecretResolver;
import org.pangrova.spi.connector.metadata.ConnectorOutputHandleCodec;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;

import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Production Worker-local connector runtime. Catalog revisions are fetched from the authenticated
 * Coordinator endpoint, secret references are resolved locally, and exact Coordinator output
 * handles are leased per task.
 */
@ThreadSafe
public final class WorkerCatalogRuntime implements CatalogResourceResolver, AutoCloseable {

    static final int MAX_CACHED_CATALOG_REVISIONS = 1024;
    private static final Pattern SECRET_REFERENCE = Pattern.compile("\\$\\{([A-Z]+):([^{}]+)}");

    @FunctionalInterface
    public interface DeploymentSource {
        CatalogDeploymentResponse fetch(String catalog, String incarnation, long revision);
    }

    private final DeploymentSource deployments;
    private final Map<String, ConnectorFactory> factories;
    private final Map<String, ConnectorSecretResolver> secretResolvers;
    private final Object catalogLock = new Object();

    /** Access-ordered cache; all accesses are guarded by catalogLock. */
    private final LinkedHashMap<CatalogRevisionKey, CatalogEntry> catalogs =
            new LinkedHashMap<>(16, 0.75f, true);

    /** Eviction transfers ownership here until shutdown actually completes. */
    private final java.util.Set<CatalogEntry> retiringCatalogs =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

    private final ConcurrentHashMap<String, TaskBinding> taskBindings = new ConcurrentHashMap<>();
    private boolean closed;

    /** Creates a runtime from trusted deployment and plugin inventories. */
    public WorkerCatalogRuntime(
            DeploymentSource deployments,
            List<ConnectorFactory> factories,
            List<ConnectorSecretResolver> secretResolvers) {
        this.deployments = java.util.Objects.requireNonNull(deployments, "deployments");
        this.factories = uniqueFactories(factories);
        this.secretResolvers = uniqueSecretResolvers(secretResolvers);
        if (this.factories.isEmpty()) {
            throw new IllegalStateException("No Worker connector plugins are installed");
        }
    }

    /** Discovers connector and secret-provider plugins from the Worker class loader. */
    public static WorkerCatalogRuntime discover(
            String coordinatorUrl, String internalAuthSecret, ClassLoader classLoader) {
        CatalogDeploymentClient client =
                new CatalogDeploymentClient(coordinatorUrl, internalAuthSecret);
        List<ConnectorFactory> factories =
                ServiceLoader.load(ConnectorFactory.class, classLoader).stream()
                        .map(ServiceLoader.Provider::get)
                        .toList();
        List<ConnectorSecretResolver> discoveredSecrets =
                ServiceLoader.load(ConnectorSecretResolver.class, classLoader).stream()
                        .map(ServiceLoader.Provider::get)
                        .toList();
        java.util.ArrayList<ConnectorSecretResolver> secrets =
                new java.util.ArrayList<>(discoveredSecrets);
        if (secrets.stream().noneMatch(resolver -> "ENV".equals(resolver.prefix()))) {
            secrets.add(new EnvironmentConnectorSecretResolver());
        }
        return new WorkerCatalogRuntime(client::fetch, factories, secrets);
    }

    @Override
    public void bindTask(String taskId, TaskRequest request) {
        requireTaskIdentity(taskId, request);
        var write = request.getExecutionOptions().getWrite();
        if (!write.hasOutputBinding()
                || write.getOutputBinding().getBindingCase()
                        == org.pangrova.protocol.v2.WriteOutputBinding.BindingCase
                                .BINDING_NOT_SET) {
            throw new IllegalArgumentException(
                    "Write task requires a connector-owned output binding");
        }
        boolean nativeSink =
                write.getOutputBinding().hasLance() || write.getOutputBinding().hasPhysical();
        CatalogExecutionBinding catalogBinding =
                WorkerWriteOutputs.catalog(write.getOutputBinding());
        String tenantId = request.getSecurity().getTenantId();
        // Connector instances hold locally resolved credentials. A deployment
        // shared by tenants must never share the first tenant's connector.
        CatalogRevisionKey key = CatalogRevisionKey.from(catalogBinding, tenantId);
        CatalogEntry entry = acquireCatalog(key, catalogBinding, tenantId);
        boolean bound = false;
        try {
            CatalogRuntime runtime = awaitCatalog(entry);
            validateTenant(runtime.deployment(), tenantId);
            TaskConnectorSession session =
                    TaskConnectorSession.from(
                            request.getSecurity(),
                            request.getAttempt(),
                            catalogBinding.getCatalogName(),
                            request.getExecutionOptions().getSessionPropertiesMap());
            SerializedConnectorOutputHandle serialized =
                    WorkerWriteOutputs.outputHandle(write.getOutputBinding());
            ConnectorOutputTableHandle outputHandle = decodeHandle(runtime.codec(), serialized);
            if (!runtime.codec()
                    .writeColumnNames(outputHandle)
                    .equals(requireWriteColumns(serialized))) {
                throw new IllegalArgumentException(
                        "Output handle columns differ from pinned columns");
            }
            validateNativeOutput(runtime, session, request, outputHandle);
            CatalogResources resources =
                    taskResources(runtime, session, outputHandle, serialized, nativeSink);
            synchronized (catalogLock) {
                requireOpen();
                TaskBinding previous =
                        taskBindings.putIfAbsent(
                                taskId,
                                new TaskBinding(entry, catalogBinding.getCatalogName(), resources));
                if (previous != null) {
                    throw new IllegalStateException(
                            "Task connector binding already exists: " + taskId);
                }
                bound = true;
            }
        } finally {
            if (!bound) {
                releaseCatalog(entry);
            }
        }
    }

    private static void validateNativeOutput(
            CatalogRuntime runtime,
            TaskConnectorSession session,
            TaskRequest request,
            ConnectorOutputTableHandle handle) {
        var binding = request.getExecutionOptions().getWrite().getOutputBinding();
        if (binding.hasLance() || binding.hasPhysical()) {
            NativeOutputAllocationValidation.validateTask(request, handle);
        }
        if (binding.hasPhysical()) {
            runtime.connector()
                    .getNativePhysicalWriteProvider()
                    .orElseThrow(
                            () ->
                                    new IllegalArgumentException(
                                            "Connector has no Native physical write metadata provider"))
                    .validateAllocation(
                            session, handle, handle.nativePhysicalWriteAllocation().orElseThrow());
        }
    }

    private static CatalogResources taskResources(
            CatalogRuntime runtime,
            TaskConnectorSession session,
            ConnectorOutputTableHandle handle,
            SerializedConnectorOutputHandle serialized,
            boolean nativeSink) {
        if (nativeSink) {
            return null;
        }
        return new CatalogResources(
                runtime.connector().getMetadata(session),
                runtime.connector().getPageSinkProvider(),
                session,
                handle,
                requireWriteColumns(serialized));
    }

    @Override
    public CatalogResources resolve(String taskId) {
        TaskBinding binding = taskBindings.get(taskId);
        if (binding == null) {
            throw new IllegalStateException("Task connector binding not found: " + taskId);
        }
        if (binding.resources() == null) {
            throw new IllegalStateException(
                    "Native dataset task cannot resolve a connector PageSink");
        }
        return binding.resources();
    }

    @Override
    public void releaseTask(String taskId) {
        TaskBinding binding = taskBindings.remove(taskId);
        if (binding != null) {
            releaseCatalog(binding.entry());
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

    public int cachedCatalogRevisionCount() {
        synchronized (catalogLock) {
            return catalogs.size();
        }
    }

    /** Leases an exact tenant deployment for metadata control, without acquiring a PageSink. */
    public CatalogControlLease leaseControl(
            CatalogExecutionBinding binding,
            org.pangrova.protocol.v2.SecurityContext security,
            java.util.Optional<org.pangrova.protocol.v2.WriteOutputBinding> output) {
        String tenant = security.getTenantId();
        if (tenant.isBlank() || security.getPrincipal().isBlank()) {
            throw new IllegalArgumentException(
                    "Native dataset control requires tenant and principal");
        }
        CatalogEntry entry =
                acquireCatalog(CatalogRevisionKey.from(binding, tenant), binding, tenant);
        boolean leased = false;
        try {
            CatalogRuntime runtime = awaitCatalog(entry);
            requireDeploymentIdentity(binding, runtime.deployment());
            validateTenant(runtime.deployment(), tenant);
            output.ifPresent(value -> validateControlOutput(runtime, value));
            CatalogControlLease lease =
                    new CatalogControlLease(() -> releaseCatalog(entry), runtime.storage());
            leased = true;
            return lease;
        } finally {
            if (!leased) {
                releaseCatalog(entry);
            }
        }
    }

    private static void validateControlOutput(
            CatalogRuntime runtime, org.pangrova.protocol.v2.WriteOutputBinding binding) {
        if (!binding.hasLance() && !binding.hasPhysical()) {
            throw new IllegalArgumentException("Publication requires a Native output binding");
        }
        var serialized = WorkerWriteOutputs.outputHandle(binding);
        var handle = decodeHandle(runtime.codec(), serialized);
        if (!runtime.codec().writeColumnNames(handle).equals(requireWriteColumns(serialized))) {
            throw new IllegalArgumentException("Output handle columns differ from pinned columns");
        }
        if (binding.hasPhysical()) {
            NativeOutputAllocationValidation.validate(binding.getPhysical(), handle);
        } else {
            NativeOutputAllocationValidation.validate(binding.getLance(), handle);
        }
    }

    private CatalogEntry acquireCatalog(
            CatalogRevisionKey key, CatalogExecutionBinding binding, String tenantId) {
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

    private static CatalogRuntime awaitCatalog(CatalogEntry entry) {
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

    private void releaseCatalog(CatalogEntry entry) {
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
        ConnectorFactory factory = factories.get(deployment.connectorProvider());
        if (factory == null) {
            throw new IllegalStateException(
                    "Connector plugin is not installed: " + deployment.connectorProvider());
        }
        Map<String, String> resolved = resolveSecrets(deployment.properties(), tenantId);
        Connector connector =
                factory.create(deployment.catalogName(), resolved, new WorkerConnectorContext());
        entry.shutdown.attach(connector);
        ConnectorOutputHandleCodec codec = connector.getOutputHandleCodec();
        var storage =
                java.util.Set.of("lance", "paimon").contains(deployment.connectorProvider())
                        ? NativeObjectStoreBindings.create(binding, tenantId, resolved)
                        : null;
        return new CatalogRuntime(deployment, connector, codec, storage);
    }

    private Map<String, String> resolveSecrets(Map<String, String> properties, String tenantId) {
        Map<String, String> resolved = new LinkedHashMap<>();
        properties.forEach((key, value) -> resolved.put(key, resolveSecretValue(value, tenantId)));
        return Map.copyOf(resolved);
    }

    private String resolveSecretValue(String value, String tenantId) {
        if (value == null || !value.contains("${")) {
            return value;
        }
        Matcher matcher = SECRET_REFERENCE.matcher(value);
        StringBuffer target = new StringBuffer();
        while (matcher.find()) {
            ConnectorSecretResolver resolver = secretResolvers.get(matcher.group(1));
            if (resolver == null) {
                throw new IllegalStateException(
                        "No Worker secret resolver for prefix " + matcher.group(1));
            }
            String secret = resolver.resolve(tenantId, matcher.group(2));
            if (secret == null) {
                throw new IllegalStateException("Secret resolver returned null");
            }
            matcher.appendReplacement(target, Matcher.quoteReplacement(secret));
        }
        matcher.appendTail(target);
        if (target.indexOf("${") >= 0) {
            throw new IllegalStateException("Malformed connector secret reference");
        }
        return target.toString();
    }

    private static ConnectorOutputTableHandle decodeHandle(
            ConnectorOutputHandleCodec codec, SerializedConnectorOutputHandle serialized) {
        if (!codec.typeUrl().equals(serialized.getTypeUrl())
                || codec.codecVersion() != serialized.getCodecVersion()
                || !checksumMatches(serialized)) {
            throw new IllegalArgumentException("Connector output handle envelope mismatch");
        }
        return codec.deserialize(
                serialized.getCodecVersion(), serialized.getPayload().toByteArray());
    }

    private static boolean checksumMatches(SerializedConnectorOutputHandle serialized) {
        var checksum = serialized.getPayloadChecksum();
        return checksum.getAlgorithm()
                        == org.pangrova.protocol.v2.Checksum.Algorithm.ALGORITHM_SHA256
                && checksum.getDigest().size() == 32
                && MessageDigest.isEqual(
                        checksum.getDigest().toByteArray(),
                        CatalogDeploymentFingerprint.sha256(serialized.getPayload().toByteArray())
                                .getDigest()
                                .toByteArray());
    }

    private static List<String> requireWriteColumns(SerializedConnectorOutputHandle handle) {
        List<String> names = List.copyOf(handle.getWriteColumnNamesList());
        if (names.isEmpty()
                || names.stream().anyMatch(name -> name == null || name.isBlank())
                || names.stream().distinct().count() != names.size()) {
            throw new IllegalArgumentException(
                    "Connector output handle requires unique write columns");
        }
        return names;
    }

    private static void requireTaskIdentity(String taskId, TaskRequest request) {
        if (taskId == null
                || taskId.isBlank()
                || !taskId.equals(request.getAttempt().getTaskId())
                || request.getAttempt().getQueryOrJobId().isBlank()
                || request.getSecurity().getTenantId().isBlank()
                || request.getSecurity().getPrincipal().isBlank()) {
            throw new IllegalArgumentException(
                    "Connector write requires exact task and security identity");
        }
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

    private static Map<String, ConnectorFactory> uniqueFactories(List<ConnectorFactory> values) {
        Map<String, ConnectorFactory> result = new LinkedHashMap<>();
        for (ConnectorFactory value : values) {
            if (value == null
                    || value.getName() == null
                    || value.getName().isBlank()
                    || result.putIfAbsent(value.getName(), value) != null) {
                throw new IllegalStateException("Invalid or duplicate Worker connector factory");
            }
        }
        return Map.copyOf(result);
    }

    private static Map<String, ConnectorSecretResolver> uniqueSecretResolvers(
            List<ConnectorSecretResolver> values) {
        Map<String, ConnectorSecretResolver> result = new LinkedHashMap<>();
        for (ConnectorSecretResolver value : values) {
            if (value == null
                    || value.prefix() == null
                    || value.prefix().isBlank()
                    || result.putIfAbsent(value.prefix(), value) != null) {
                throw new IllegalStateException("Invalid or duplicate Worker secret resolver");
            }
        }
        return Map.copyOf(result);
    }

    private record CatalogRuntime(
            CatalogDeploymentResponse deployment,
            Connector connector,
            ConnectorOutputHandleCodec codec,
            org.pangrova.protocol.v2.ObjectStoreExecutionBinding storage) {}

    private static final class CatalogEntry {
        private final CompletableFuture<CatalogRuntime> ready = new CompletableFuture<>();
        private final CatalogShutdownOwner shutdown = new CatalogShutdownOwner();
        private int leases;
    }

    private record TaskBinding(
            CatalogEntry entry, String catalogName, CatalogResources resources) {}

    private static final class WorkerConnectorContext implements ConnectorContext {
        @Override
        public java.util.Optional<org.pangrova.spi.connector.metadata.NativeDatasetControlPlane>
                nativeDatasetControlPlane() {
            return java.util.Optional.empty();
        }
    }
}
