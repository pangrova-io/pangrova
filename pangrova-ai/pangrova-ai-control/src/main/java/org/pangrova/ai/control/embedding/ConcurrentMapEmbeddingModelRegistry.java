package org.pangrova.ai.control.embedding;

import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.security.api.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.pangrova.common.api.annotation.ThreadSafe;
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

    private final ConcurrentMap<String, RegisteredModel> models = new ConcurrentHashMap<>();
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
    public void authorizeCreate(String name, UserIdentity user) {
        requireAdmin(user, "CREATE", name);
    }

    @Override
    public EmbeddingModelAlterTarget prepareAlter(String name, UserIdentity user) {
        requireAdmin(user, "ALTER", name);
        RegisteredModel model = models.get(name);
        if (model == null) {
            throw new ModelNotFoundException(name);
        }
        return model.alterTarget();
    }

    @Override
    public void create(EmbeddingModelDescriptor descriptor, UserIdentity user) {
        authorizeCreate(descriptor.name(), user);
        if (descriptor.version() != 0) {
            throw new IllegalArgumentException("New model version must be 0, got: " + descriptor.version());
        }

        RegisteredModel existing = models.putIfAbsent(
                descriptor.name(), RegisteredModel.create(descriptor));
        if (existing != null) {
            throw new ModelAlreadyExistsException(descriptor.name());
        }
        LOG.info("Embedding model created: name={}, provider={}, dimension={}, user={}",
                descriptor.name(), descriptor.provider(), descriptor.dimension(), user.principal());
    }

    @Override
    public EmbeddingModelDescriptor alter(
            EmbeddingModelAlterTarget target,
            EmbeddingModelDescriptor replacement, UserIdentity user) {
        String name = target.descriptor().name();
        requireAdmin(user, "ALTER", name);

        RegisteredModel current = models.get(name);
        if (current == null) {
            throw new ModelNotFoundException(name);
        }
        EmbeddingModelRevision.requireValidReplacement(
                name, current.descriptor(), replacement, target.descriptor().version());
        if (!current.incarnationId().equals(target.incarnationId())) {
            throw new VersionConflictException(
                    name, target.descriptor().version(), current.descriptor().version());
        }
        RegisteredModel updated = new RegisteredModel(replacement, current.incarnationId());
        boolean replaced = models.replace(name, current, updated);
        if (!replaced) {
            RegisteredModel actual = models.get(name);
            int actualVersion = actual != null ? actual.descriptor().version() : -1;
            throw new VersionConflictException(
                    name, target.descriptor().version(), actualVersion);
        }

        LOG.info("Embedding model altered: name={}, version={}->{}, user={}",
                name, target.descriptor().version(), replacement.version(), user.principal());
        return replacement;
    }

    @Override
    public void drop(String name, boolean cascade, UserIdentity user) {
        requireAdmin(user, "DROP", name);

        RegisteredModel existing = models.get(name);
        if (existing == null) {
            throw new ModelNotFoundException(name);
        }

        throw new ModelDropUnavailableException(name);
    }

    @Override
    public Optional<EmbeddingModelDescriptor> find(String name, UserIdentity user) {
        requireUsage(user, "FIND", name);
        return Optional.ofNullable(models.get(name)).map(RegisteredModel::descriptor);
    }

    @Override
    public List<EmbeddingModelDescriptor> listAll(UserIdentity user) {
        if (!hasUsagePrivilege(user)) {
            return List.of();
        }
        return models.values().stream().map(RegisteredModel::descriptor).toList();
    }

    @Override
    public void validateDimension(String modelName, int columnDimension) {
        RegisteredModel model = models.get(modelName);
        if (model == null) {
            throw new ModelNotFoundException(modelName);
        }
        EmbeddingModelDescriptor descriptor = model.descriptor();
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

    private record RegisteredModel(
            EmbeddingModelDescriptor descriptor,
            String incarnationId) {

        private static RegisteredModel create(EmbeddingModelDescriptor descriptor) {
            return new RegisteredModel(descriptor, java.util.UUID.randomUUID().toString());
        }

        private EmbeddingModelAlterTarget alterTarget() {
            return new EmbeddingModelAlterTarget(descriptor, incarnationId);
        }
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
