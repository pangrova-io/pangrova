package org.pangrova.coordinator.ai.embedding;

import org.pangrova.spi.ai.embedding.EmbeddingModelDescriptor;
import org.pangrova.spi.security.UserIdentity;

import java.util.List;
import java.util.Optional;

/**
 * Catalog-level registry for embedding model configurations.
 *
 * <p>Manages the lifecycle of registered embedding models (CREATE, ALTER, DROP, SHOW)
 * with version tracking and RBAC enforcement. Model configurations are stored as
 * {@link EmbeddingModelDescriptor} instances; actual credentials are never persisted
 * (only opaque secret references).</p>
 *
 * <p>Thread safety: implementations must be safe for concurrent use.</p>
 */
public interface EmbeddingModelRegistry {

    /**
     * Authorizes CREATE before the Coordinator resolves any Worker deployment metadata.
     *
     * @param name model name requested by SQL
     * @param user authenticated user performing the operation
     * @throws EmbeddingRegistryAccessDeniedException if user lacks ADMIN privilege
     */
    void authorizeCreate(String name, UserIdentity user);

    /**
     * Authorizes ALTER and captures the exact catalog incarnation before any
     * Worker deployment metadata is resolved.
     */
    EmbeddingModelAlterTarget prepareAlter(String name, UserIdentity user);

    /**
     * Registers a new embedding model configuration.
     * Requires ADMIN privilege on the caller.
     *
     * @param descriptor the model configuration (version must be 0 for new models)
     * @param user       the authenticated user performing the operation
     * @throws ModelAlreadyExistsException if a model with the same name already exists
     * @throws EmbeddingRegistryAccessDeniedException if user lacks ADMIN privilege
     */
    void create(EmbeddingModelDescriptor descriptor, UserIdentity user);

    /**
     * Updates an existing model configuration with optimistic concurrency.
     * Increments the version atomically. In-flight requests complete with the
     * version at request start; new requests use the updated configuration.
     * Requires ADMIN privilege on the caller.
     *
     * @param target          exact name/version/incarnation captured by {@link #prepareAlter}
     * @param replacement     complete next revision, already bound to an exact Worker deployment
     * @param user            the authenticated user performing the operation
     * @return the updated descriptor with incremented version
     * @throws ModelNotFoundException if no model with the given name exists
     * @throws VersionConflictException if the current version does not match expectedVersion
     * @throws EmbeddingRegistryAccessDeniedException if user lacks ADMIN privilege
     */
    EmbeddingModelDescriptor alter(
            EmbeddingModelAlterTarget target,
            EmbeddingModelDescriptor replacement,
            UserIdentity user);

    /**
     * Requests model removal. Currently rejects both modes without mutation until
     * generated-column definitions have a durable dependency authority.
     * Requires ADMIN privilege on the caller.
     *
     * @param name    the model name to drop
     * @param cascade requested dependency behavior; currently both values fail closed because
     *                connector table definitions and the registry lack one atomic authority
     * @param user    the authenticated user performing the operation
     * @throws ModelNotFoundException if no model with the given name exists
     * @throws ModelDropUnavailableException until durable dependency coordination is available
     * @throws EmbeddingRegistryAccessDeniedException if user lacks ADMIN privilege
     */
    void drop(String name, boolean cascade, UserIdentity user);

    /**
     * Retrieves a model descriptor by name.
     * Requires USAGE privilege on the caller.
     *
     * @param name the model name
     * @param user the authenticated user performing the operation
     * @return the descriptor if found
     * @throws EmbeddingRegistryAccessDeniedException if user lacks USAGE privilege
     */
    Optional<EmbeddingModelDescriptor> find(String name, UserIdentity user);

    /**
     * Lists all registered embedding models visible to the user.
     *
     * @param user the authenticated user performing the operation
     * @return list of model descriptors the user has USAGE privilege on
     */
    List<EmbeddingModelDescriptor> listAll(UserIdentity user);

    /**
     * Validates that a model's output dimension matches the expected column dimension.
     *
     * @param modelName       the model name to validate
     * @param columnDimension the expected VECTOR column dimension
     * @throws ModelNotFoundException if no model with the given name exists
     * @throws DimensionMismatchException if model dimension != columnDimension
     */
    void validateDimension(String modelName, int columnDimension);
}
