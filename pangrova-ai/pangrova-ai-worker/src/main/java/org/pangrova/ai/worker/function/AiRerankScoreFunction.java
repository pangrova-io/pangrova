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
package org.pangrova.ai.worker.function;

import org.pangrova.ai.api.reranking.RerankingProvider;
import org.pangrova.types.value.ColumnValue;
import org.pangrova.function.api.scalar.BatchableScalarFunction;
import org.pangrova.function.api.metadata.FunctionMetadata;
import org.pangrova.ai.worker.context.AiExecutionContext;
import org.pangrova.ai.worker.context.BoundAiInvocation;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Worker-local governed scoring primitive used by the Native rerank relation. */
public final class AiRerankScoreFunction implements BatchableScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("internal", "rerank_score");
    public static final String NAME = METADATA.name();

    private final Function<String, RerankingProvider> providerLookup;

    AiRerankScoreFunction(
            Function<String, RerankingProvider> providerLookup) {
        if (providerLookup == null) {
            throw new IllegalArgumentException("providerLookup required");
        }
        this.providerLookup = providerLookup;
    }

    @Override
    public FunctionMetadata getMetadata() {
        return METADATA;
    }

    @Override
    public ColumnValue invoke(List<ColumnValue> arguments) {
        requireProviderBudget();
        Values values = values(arguments);
        if (values == null) {
            return new ColumnValue.NullValue();
        }
        RerankingProvider provider = requireProvider(
                values.model(), values.binding());
        return score(AiExecutionContext.callWithBoundInvocation(
                values.binding(),
                () -> provider.score(
                        values.query(), List.of(values.document())))
                .scores().get(0));
    }

    @Override
    public List<ColumnValue> invokeBatch(
            List<List<ColumnValue>> argumentsBatch) {
        requireProviderBudget();
        ColumnValue[] results = new ColumnValue[argumentsBatch.size()];
        java.util.Arrays.fill(results, new ColumnValue.NullValue());
        Map<BatchKey, List<Item>> grouped = new HashMap<>();
        for (int index = 0; index < argumentsBatch.size(); ++index) {
            Values values = values(argumentsBatch.get(index));
            if (values != null) {
                grouped.computeIfAbsent(
                                new BatchKey(values.binding(), values.query()),
                                ignored -> new ArrayList<>())
                        .add(new Item(index, values.document()));
            }
        }
        for (Map.Entry<BatchKey, List<Item>> entry : grouped.entrySet()) {
            RerankingProvider provider = requireProvider(
                    entry.getKey().binding().modelId(),
                    entry.getKey().binding());
            int maxBatch = provider.maxBatchSize();
            if (maxBatch <= 0) {
                throw new IllegalStateException(
                        "Reranking provider reported a non-positive batch size");
            }
            List<Item> items = entry.getValue();
            for (int offset = 0; offset < items.size(); offset += maxBatch) {
                int end = Math.min(items.size(), offset + maxBatch);
                List<Item> chunk = items.subList(offset, end);
                List<Double> scores = AiExecutionContext
                        .callWithBoundInvocation(
                                entry.getKey().binding(),
                                () -> provider.score(
                                        entry.getKey().query(),
                                        chunk.stream().map(Item::document).toList()))
                        .scores();
                if (scores.size() != chunk.size()) {
                    throw new IllegalStateException(
                            "Reranking provider returned an invalid batch cardinality");
                }
                for (int item = 0; item < chunk.size(); ++item) {
                    results[chunk.get(item).row()] = score(scores.get(item));
                }
            }
        }
        return List.of(results);
    }

    private static void requireProviderBudget() {
        AiExecutionContext context = AiExecutionContext.current().orElseThrow(
                () -> new IllegalStateException(
                        "Reranking requires a trusted AI execution context"));
        context.requireGovernedIdentity();
        context.requireProviderBudget();
    }

    private RerankingProvider requireProvider(
            String model, BoundAiInvocation binding) {
        RerankingProvider provider = providerLookup.apply(model);
        if (provider == null) {
            throw new IllegalStateException(
                    "Reranking model not found: " + model);
        }
        binding.requireExactProvider(
                provider.modelName(), provider.modelVersion(),
                provider.modelSignatureSha256(),
                org.pangrova.ai.api.capability.AiCapabilityDescriptor.Operation.RERANKING);
        return provider;
    }

    private static ColumnValue score(double value) {
        if (!Double.isFinite(value)) {
            throw new IllegalStateException(
                    "Reranking provider returned a non-finite score");
        }
        return new ColumnValue.DoubleValue(value);
    }

    private static Values values(List<ColumnValue> arguments) {
        var decoded = new GovernedAiBindingArguments(arguments, "AI_RERANK");
        String model = decoded.text("model_id");
        String query = decoded.nullableText("query");
        String document = decoded.nullableText("document");
        BoundAiInvocation binding = decoded.binding(
                org.pangrova.ai.api.capability.AiCapabilityDescriptor.Operation.RERANKING);
        return query == null || document == null
                ? null
                : new Values(model, query, document, binding);
    }

    private record BatchKey(BoundAiInvocation binding, String query) {}

    private record Item(int row, String document) {}

    private record Values(
            String model,
            String query,
            String document,
            BoundAiInvocation binding) {}
}
