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
package org.pangrova.worker.ai.function;

import org.pangrova.worker.ai.function.file.FileStructHelper;

import org.pangrova.spi.ai.inference.GenerationParameters;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.ai.inference.InferenceResult;
import org.pangrova.spi.ai.inference.MultimodalPrompt;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.function.BatchableScalarFunction;
import org.pangrova.spi.function.FunctionMetadata;
import org.pangrova.protocol.function.declaration.FunctionBindingDeclarations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Scalar SQL function: {@code ai_complete(model, prompt[, file_or_json_mode]) -> VARCHAR}.
 *
 * <p>Invokes the generative inference provider on the Worker data plane to
 * produce a text completion for each row. Supports two optional third-argument modes:
 * <ul>
 *   <li>If the third argument is a ROW (FILE struct) &rarr; multimodal prompt (text + file)</li>
 *   <li>If the third argument is VARCHAR {@code 'true'} &rarr; JSON-formatted output</li>
 * </ul>
 * <pre>
 *   SELECT ai_complete('gpt-4o', 'Summarize: ' || body) FROM articles
 *   SELECT ai_complete('gpt-4o', 'Return JSON: ' || body, 'true') FROM articles
 *   SELECT ai_complete('gpt-4o', 'Describe this image', photo) FROM products
 * </pre>
 *
 * <p>Thread-safe: delegates to thread-safe InferenceProvider.</p>
 */
public final class AiCompletionFunction implements BatchableScalarFunction {

    private static final FunctionMetadata METADATA =
            FunctionBindingDeclarations.callbackMetadata("ai_complete");

    private final java.util.function.Function<String, InferenceProvider> providerLookup;

    /**
     * Creates a completion function backed by the given provider lookup.
     *
     * @param providerLookup resolves model names to inference providers
     */
    AiCompletionFunction(
            java.util.function.Function<String, InferenceProvider> providerLookup) {
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
    public List<List<String>> getArgumentSignatures() {
        return FunctionBindingDeclarations.callbackArgumentSignatures("ai_complete");
    }

    @Override
    public ColumnValue invoke(List<ColumnValue> arguments) {
        if (arguments.size() < 2) {
            throw new IllegalArgumentException("ai_complete requires 2 arguments: model, prompt");
        }
        String modelName = extractString(arguments.get(0), "model");
        String prompt = extractString(arguments.get(1), "prompt");
        if (modelName == null || prompt == null) {
            return new ColumnValue.NullValue();
        }

        InferenceProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + modelName);
        }

        // Check if third argument is a FILE (ROW) for multimodal completion
        if (arguments.size() >= 3 && arguments.get(2) instanceof ColumnValue.RowValue) {
            ColumnValue fileArg = arguments.get(2);
            MultimodalPrompt multiPrompt = FileStructHelper.buildMultimodalPrompt(prompt, fileArg);
            InferenceResult multiResult = provider.completeMultimodal(multiPrompt, buildParams(arguments));
            return new ColumnValue.StringValue(multiResult.text());
        }

        String result = provider.complete(prompt, buildParams(arguments)).text();
        return new ColumnValue.StringValue(result);
    }

    @Override
    public List<ColumnValue> invokeBatch(List<List<ColumnValue>> argumentsBatch) {
        BatchInputs inputs = collectBatchInputs(argumentsBatch);
        ColumnValue[] results = emptyResults(argumentsBatch.size());
        completeMultimodal(argumentsBatch, results);
        for (Map.Entry<BatchKey, List<Integer>> entry : inputs.indices().entrySet()) {
            completeForModel(entry.getKey(), entry.getValue(), inputs.prompts(), results);
        }
        return List.of(results);
    }

    private static BatchInputs collectBatchInputs(List<List<ColumnValue>> argumentsBatch) {
        Map<BatchKey, List<Integer>> batchToIndices = new HashMap<>();
        String[] prompts = new String[argumentsBatch.size()];
        String[] models = new String[argumentsBatch.size()];
        GenerationParameters[] params = new GenerationParameters[argumentsBatch.size()];
        for (int i = 0; i < argumentsBatch.size(); i++) {
            List<ColumnValue> args = argumentsBatch.get(i);
            if (args.size() < 2) {
                throw new IllegalArgumentException("ai_complete requires 2 arguments: model, prompt");
            }
            models[i] = extractString(args.get(0), "model");
            prompts[i] = extractString(args.get(1), "prompt");
            params[i] = buildParams(args);
            boolean multimodal = args.size() == 3
                    && args.get(2) instanceof ColumnValue.RowValue;
            if (!multimodal && models[i] != null && prompts[i] != null) {
                batchToIndices.computeIfAbsent(
                        new BatchKey(models[i], params[i]), k -> new ArrayList<>()).add(i);
            }
        }
        return new BatchInputs(batchToIndices, prompts);
    }

    private static ColumnValue[] emptyResults(int size) {
        ColumnValue[] results = new ColumnValue[size];
        for (int i = 0; i < results.length; i++) {
            results[i] = new ColumnValue.NullValue();
        }
        return results;
    }

    private void completeMultimodal(
            List<List<ColumnValue>> argumentsBatch, ColumnValue[] results) {
        for (int i = 0; i < argumentsBatch.size(); i++) {
            List<ColumnValue> args = argumentsBatch.get(i);
            if (args.size() == 3 && args.get(2) instanceof ColumnValue.RowValue) {
                results[i] = invoke(args);
            }
        }
    }

    private void completeForModel(BatchKey key, List<Integer> indices, String[] prompts,
                                  ColumnValue[] results) {
        InferenceProvider provider = providerLookup.apply(key.modelName());
        if (provider == null) {
            throw new IllegalStateException("Inference model not found: " + key.modelName());
        }
        List<String> batchPrompts = indices.stream().map(i -> prompts[i]).toList();
        List<InferenceResult> completions = provider.completeBatch(batchPrompts, key.parameters());
        if (completions == null || completions.size() != indices.size()) {
            throw new IllegalStateException("Inference provider '" + key.modelName()
                    + "' returned " + (completions == null ? "null" : completions.size())
                    + " results for " + indices.size() + " prompts");
        }
        for (int j = 0; j < indices.size(); j++) {
            results[indices.get(j)] = new ColumnValue.StringValue(completions.get(j).text());
        }
    }

    private static GenerationParameters buildParams(List<ColumnValue> arguments) {
        if (arguments.size() >= 3) {
            ColumnValue third = arguments.get(2);
            // Skip ROW values (FILE struct) — they are handled as multimodal input
            if (third instanceof ColumnValue.RowValue) {
                return GenerationParameters.DEFAULTS;
            }
            String jsonMode = extractString(third, "json_mode");
            if ("true".equalsIgnoreCase(jsonMode)) {
                return GenerationParameters.deterministicJson();
            }
        }
        return GenerationParameters.DEFAULTS;
    }

    private static String extractString(ColumnValue cv, String argName) {
        if (cv instanceof ColumnValue.StringValue sv) {
            return sv.value();
        }
        if (cv instanceof ColumnValue.NullValue) {
            return null;
        }
        throw new IllegalArgumentException(
                "ai_complete: " + argName + " must be VARCHAR, got: " + cv.getClass().getSimpleName());
    }

    private record BatchKey(String modelName, GenerationParameters parameters) {}

    private record BatchInputs(Map<BatchKey, List<Integer>> indices, String[] prompts) {}
}
