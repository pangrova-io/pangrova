package org.pangrova.coordinator.query.execution.dispatch.task;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.ai.control.model.EmbeddingModelExecutionAdmission;
import org.pangrova.ai.control.embedding.EmbeddingModelRegistry;
import org.pangrova.ai.control.embedding.EmbeddingModelRevision;
import org.pangrova.ai.control.embedding.ModelUpdateRequest;
import org.pangrova.ai.control.embedding.VersionConflictException;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.statement.StatementCategory;
import org.pangrova.query.core.dispatch.StatementExecutionContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.parser.api.ast.statement.AlterEmbeddingModelStatement;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.ModelAvailability;
import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.types.value.TypedRow;
import org.pangrova.security.api.identity.UserIdentity;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/** Executes a version-fenced ALTER and atomically persists a newly bound Worker deployment. */
public final class AlterEmbeddingModelTask implements MetadataTask<AlterEmbeddingModelStatement> {

    private static final String EXPECTED_VERSION = "expected_version";
    private static final Set<String> SUPPORTED_PROPERTIES = Set.of(
            EXPECTED_VERSION, "endpoint", "api_key_secret", "max_batch_size",
            "max_requests_per_second", "query_prefix", "document_prefix");

    private final EmbeddingModelRegistry registry;
    private final BiFunction<String, CapabilityRef, ModelAvailability> modelResolver;

    public AlterEmbeddingModelTask(
            EmbeddingModelRegistry registry,
            BiFunction<String, CapabilityRef, ModelAvailability> modelResolver) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.modelResolver = Objects.requireNonNull(
                modelResolver, "modelResolver must not be null");
    }

    @Override
    public StatementResult execute(
            AlterEmbeddingModelStatement statement,
            StatementExecutionContext context) {
        UserIdentity user = requireIdentity(context);
        org.pangrova.ai.control.embedding.EmbeddingModelAlterTarget target =
                registry.prepareAlter(statement.modelName(), user);
        Map<String, String> properties = statement.properties();
        requireSupportedProperties(properties);
        int expectedVersion = parsePositiveOrZero(
                requireProperty(properties, EXPECTED_VERSION), EXPECTED_VERSION);
        ModelUpdateRequest update = updateRequest(properties);
        EmbeddingModelDescriptor current = target.descriptor();
        if (current.version() != expectedVersion) {
            throw new VersionConflictException(
                    statement.modelName(), expectedVersion, current.version());
        }
        EmbeddingModelDescriptor requested = EmbeddingModelRevision.applyUpdate(current, update);
        ModelAvailability availability = modelResolver.apply(
                statement.modelName(), BuiltInCapabilityCatalog.aiEmbeddingPlanRef());
        EmbeddingModelDescriptor bound =
                EmbeddingModelExecutionAdmission.bind(requested, availability);
        EmbeddingModelDescriptor stored = registry.alter(
                target, bound, user);
        return result(stored);
    }

    private static UserIdentity requireIdentity(StatementExecutionContext context) {
        UserIdentity user = context.session() != null
                ? context.session().userIdentity() : null;
        if (user == null) {
            throw new PangrovaException(
                    PangrovaErrorCode.ACCESS_DENIED,
                    "ALTER EMBEDDING MODEL requires an authenticated session");
        }
        return user;
    }

    private static ModelUpdateRequest updateRequest(Map<String, String> properties) {
        return new ModelUpdateRequest(
                optional(properties, "endpoint"),
                optional(properties, "api_key_secret"),
                optionalInt(properties, "max_batch_size"),
                optionalInt(properties, "max_requests_per_second"),
                optional(properties, "query_prefix"),
                optional(properties, "document_prefix"));
    }

    private static Optional<String> optional(Map<String, String> properties, String name) {
        return Optional.ofNullable(properties.get(name));
    }

    private static Optional<Integer> optionalInt(
            Map<String, String> properties, String name) {
        return optional(properties, name).map(value -> parsePositiveOrZero(value, name));
    }

    private static String requireProperty(Map<String, String> properties, String name) {
        String value = properties.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Embedding model property required: " + name);
        }
        return value;
    }

    private static void requireSupportedProperties(Map<String, String> properties) {
        for (String property : properties.keySet()) {
            if (!SUPPORTED_PROPERTIES.contains(property)) {
                throw new IllegalArgumentException(
                        "Unsupported embedding model property: " + property);
            }
        }
    }

    private static int parsePositiveOrZero(String value, String property) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(
                        "Embedding model property must not be negative: " + property);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Embedding model property must be an integer: " + property, e);
        }
    }

    private static StatementResult result(EmbeddingModelDescriptor stored) {
        TypedRow row = TypedRow.builder()
                .putString("result", "Embedding model '" + stored.name()
                        + "' altered to version " + stored.version())
                .build();
        return new StatementResult(
                List.of(row), List.of("result"), null, StatementCategory.DDL);
    }
}
