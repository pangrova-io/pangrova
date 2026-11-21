/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.ai.control.model.EmbeddingModelExecutionAdmission;
import org.pangrova.ai.control.embedding.EmbeddingModelRegistry;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.query.api.enums.StatementCategory;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.parser.api.ast.statement.CreateEmbeddingModelStatement;
import org.pangrova.ai.api.embedding.EmbeddingModelDescriptor;
import org.pangrova.types.value.TypedRow;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.ModelAvailability;

/**
 * Executes CREATE EMBEDDING MODEL by registering the model in the
 * {@link EmbeddingModelRegistry}.
 */
public final class CreateEmbeddingModelTask implements MetadataTask<CreateEmbeddingModelStatement> {

    private static final Set<String> SUPPORTED_PROPERTIES = Set.of(
            "provider", "endpoint", "dimension", "authentication", "api_key_secret", "max_batch_size",
            "max_requests_per_second", "query_prefix", "document_prefix");

    private final EmbeddingModelRegistry registry;
    private final BiFunction<String, CapabilityRef, ModelAvailability> modelResolver;

    public CreateEmbeddingModelTask(
            EmbeddingModelRegistry registry,
            BiFunction<String, CapabilityRef, ModelAvailability> modelResolver) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.modelResolver = Objects.requireNonNull(
                modelResolver, "modelResolver must not be null");
    }

    @Override
    public StatementResult execute(CreateEmbeddingModelStatement statement,
                                   StatementExecutionContext context) {
        UserIdentity user = context.session() != null ? context.session().userIdentity() : null;
        if (user == null) {
            throw new PangrovaException(
                    PangrovaErrorCode.ACCESS_DENIED,
                    "CREATE EMBEDDING MODEL requires an authenticated session");
        }
        registry.authorizeCreate(statement.modelName(), user);
        Map<String, String> props = statement.properties();
        requireSupportedProperties(props);
        String provider = requireProperty(props, "provider");
        String endpoint = requireProperty(props, "endpoint");
        int dimension = parseInt(requireProperty(props, "dimension"), "dimension");
        String apiKeySecret = apiKeySecret(props);
        int maxBatchSize = parseInt(
                props.getOrDefault("max_batch_size", "32"), "max_batch_size");
        int maxRps = parseInt(props.getOrDefault("max_requests_per_second", "100"),
                "max_requests_per_second");
        String queryPrefix = props.getOrDefault("query_prefix", "");
        String documentPrefix = props.getOrDefault("document_prefix", "");

        EmbeddingModelDescriptor requested = new EmbeddingModelDescriptor(
                statement.modelName(),
                new EmbeddingModelDescriptor.ProviderRoute(
                        provider, endpoint, apiKeySecret),
                new EmbeddingModelDescriptor.EmbeddingContract(
                        dimension, queryPrefix, documentPrefix),
                new EmbeddingModelDescriptor.ServingLimits(maxBatchSize, maxRps),
                0,
                EmbeddingModelDescriptor.DeploymentFence.unbound());
        ModelAvailability availability = modelResolver.apply(
                statement.modelName(), BuiltInCapabilityCatalog.aiEmbeddingPlanRef());
        EmbeddingModelDescriptor descriptor =
                EmbeddingModelExecutionAdmission.bind(requested, availability);

        registry.create(descriptor, user);

        TypedRow row = TypedRow.builder()
                .putString("result", "Embedding model '" + statement.modelName() + "' created")
                .build();
        return new StatementResult(List.of(row), List.of("result"), null, StatementCategory.DDL);
    }

    private static String apiKeySecret(Map<String, String> props) {
        String authentication = props.getOrDefault("authentication", "bearer")
                .trim().toLowerCase(Locale.ROOT);
        if (!authentication.equals("bearer") && !authentication.equals("none")) {
            throw new IllegalArgumentException(
                    "Embedding model authentication must be 'bearer' or 'none'");
        }
        if (authentication.equals("none")) {
            if (props.containsKey("api_key_secret")) {
                throw new IllegalArgumentException(
                        "authentication=none must not declare api_key_secret");
            }
            return "";
        }
        return requireProperty(props, "api_key_secret");
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

    private static int parseInt(String value, String property) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Embedding model property must be an integer: " + property, e);
        }
    }

}
