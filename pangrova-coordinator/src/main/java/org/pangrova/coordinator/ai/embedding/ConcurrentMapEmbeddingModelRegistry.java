package org.pangrova.coordinator.ai.embedding;

import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.security.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe in-memory implementation of the embedding model registry.
 * Uses ConcurrentHashMap for lock-free reads and atomic compute operations
 * for version-tracked writes.
 *
 * <p>RBAC enforcement: ADMIN group required for mutating operations (CREATE/ALTER/DROP);
 * USAGE group required for read operations (find/listAll). The admin and usage group
 * names are injected at construction time.</p>
 */
@ThreadSafe
public final class ConcurrentMapEmbeddingModelRegistry implements EmbeddingModelRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ConcurrentMapEmbeddingModelRegistry.class);

    private final ConcurrentMap<String, EmbeddingModelDescriptor> models = new ConcurrentHashMap<>();
    private final Set<String> dependentModels = ConcurrentHashMap.newKeySet();
    private final String adminGroup;
    private final String usageGroup;

    /** Creates an in-memory registry enforcing the given admin and usage access groups. */
    public ConcurrentMapEmbeddingModelRegistry(String adminGroup, String usageGroup) {
        if (adminGroup == null || adminGroup.isBlank()) {
            throw new IllegalArgumentException("adminGroup required");
        }
        if (usageGroup == null || usageGroup.isBlank()) {
            throw new IllegalArgumentException("usageGroup required");
        }
        this.adminGroup = adminGroup;
        this.usageGroup = usageGroup;
    }

    @Override
    public void create(EmbeddingModelDescriptor descriptor, UserIdentity user) {
        requireAdmin(user, "CREATE", descriptor.name());
        if (descriptor.version() != 0) {
            throw new IllegalArgumentException("New model version must be 0, got: " + descriptor.version());
        }

        EmbeddingModelDescriptor existing = models.putIfAbsent(descriptor.name(), descriptor);
        if (existing != null) {
            throw new ModelAlreadyExistsException(descriptor.name());
        }
        LOG.info("Embedding model created: name={}, provider={}, dimension={}, user={}",
                descriptor.name(), descriptor.provider(), descriptor.dimension(), user.principal());
    }

    @Override
    public EmbeddingModelDescriptor alter(String name, ModelUpdateRequest update, int expectedVersion,
                                          UserIdentity user) {
        requireAdmin(user, "ALTER", name);
        if (!update.hasUpdates()) {
            throw new IllegalArgumentException("No fields to update in ALTER request");
        }

        EmbeddingModelDescriptor current = models.get(name);
        if (current == null) {
            throw new ModelNotFoundException(name);
        }
        if (current.version() != expectedVersion) {
            throw new VersionConflictException(name, expectedVersion, current.version());
        }

        EmbeddingModelDescriptor updated = applyUpdate(current, update);
        boolean replaced = models.replace(name, current, updated);
        if (!replaced) {
            EmbeddingModelDescriptor actual = models.get(name);
            int actualVersion = actual != null ? actual.version() : -1;
            throw new VersionConflictException(name, expectedVersion, actualVersion);
        }

        LOG.info("Embedding model altered: name={}, version={}->{}, user={}",
                name, expectedVersion, updated.version(), user.principal());
        return updated;
    }

    @Override
    public void drop(String name, boolean cascade, UserIdentity user) {
        requireAdmin(user, "DROP", name);

        EmbeddingModelDescriptor existing = models.get(name);
        if (existing == null) {
            throw new ModelNotFoundException(name);
        }

        if (!cascade && dependentModels.contains(name)) {
            throw new ModelInUseException(name);
        }

        models.remove(name);
        if (cascade) {
            dependentModels.remove(name);
        }
        LOG.info("Embedding model dropped: name={}, cascade={}, user={}", name, cascade, user.principal());
    }

    @Override
    public Optional<EmbeddingModelDescriptor> find(String name, UserIdentity user) {
        requireUsage(user, "FIND", name);
        return Optional.ofNullable(models.get(name));
    }

    @Override
    public List<EmbeddingModelDescriptor> listAll(UserIdentity user) {
        if (!hasUsagePrivilege(user)) {
            return List.of();
        }
        return List.copyOf(models.values());
    }

    @Override
    public void validateDimension(String modelName, int columnDimension) {
        EmbeddingModelDescriptor descriptor = models.get(modelName);
        if (descriptor == null) {
            throw new ModelNotFoundException(modelName);
        }
        if (descriptor.dimension() != columnDimension) {
            throw new DimensionMismatchException(modelName, descriptor.dimension(), columnDimension);
        }
    }

    /**
     * Registers a model name as having dependent auto-embedding columns.
     * Used by DDL executors when creating GENERATED ALWAYS AS columns.
     */
    public void registerDependency(String modelName) {
        dependentModels.add(modelName);
    }

    /**
     * Removes a model dependency registration.
     * Used when the last dependent column referencing this model is dropped.
     */
    public void removeDependency(String modelName) {
        dependentModels.remove(modelName);
    }

    private EmbeddingModelDescriptor applyUpdate(EmbeddingModelDescriptor current, ModelUpdateRequest update) {
        return new EmbeddingModelDescriptor(
                current.name(),
                current.provider(),
                update.endpoint().orElse(current.endpoint()),
                current.dimension(),
                update.apiKeySecret().orElse(current.apiKeySecret()),
                update.maxBatchSize().orElse(current.maxBatchSize()),
                update.maxRequestsPerSecond().orElse(current.maxRequestsPerSecond()),
                update.queryPrefix().orElse(current.queryPrefix()),
                update.documentPrefix().orElse(current.documentPrefix()),
                current.version() + 1);
    }

    private void requireAdmin(UserIdentity user, String operation, String modelName) {
        if (!user.groups().contains(adminGroup)) {
            throw new EmbeddingRegistryAccessDeniedException(user.principal(), operation, modelName);
        }
    }

    private void requireUsage(UserIdentity user, String operation, String modelName) {
        if (!hasUsagePrivilege(user)) {
            throw new EmbeddingRegistryAccessDeniedException(user.principal(), operation, modelName);
        }
    }

    private boolean hasUsagePrivilege(UserIdentity user) {
        return user.groups().contains(usageGroup) || user.groups().contains(adminGroup);
    }
}
