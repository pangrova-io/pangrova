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

import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.function.FunctionFactory;
import org.pangrova.spi.function.ScalarFunction;
import org.pangrova.worker.ai.embedding.EmbeddingProviderLookup;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Function;

/**
 * FunctionFactory that registers all generative AI SQL functions.
 *
 * <p>Provides: ai_complete, ai_summarize, ai_classify, ai_filter, ai_embed,
 * ai_extract, ai_guard, ai_predict, ai_translate. Most functions use the
 * {@link InferenceProvider} SPI; {@code ai_embed} uses {@link EmbeddingProvider}
 * via {@link EmbeddingProviderLookup}.</p>
 *
 * <p>No-arg constructor for {@link ServiceLoader} discovery. Each provider is
 * discovered lazily at construction time; functions fail at invocation time if
 * no matching provider is configured.</p>
 */
public final class AiFunctionFactory implements FunctionFactory {

    private final Function<String, InferenceProvider> providerLookup;
    private final Function<String, EmbeddingProvider> embeddingLookup;

    public AiFunctionFactory() {
        this(discoverProviderLookup(), discoverEmbeddingLookup());
    }

    public AiFunctionFactory(Function<String, InferenceProvider> providerLookup) {
        this(providerLookup, discoverEmbeddingLookup());
    }

    public AiFunctionFactory(Function<String, InferenceProvider> providerLookup,
                      Function<String, EmbeddingProvider> embeddingLookup) {
        if (providerLookup == null) {
            throw new IllegalArgumentException("providerLookup required");
        }
        if (embeddingLookup == null) {
            throw new IllegalArgumentException("embeddingLookup required");
        }
        this.providerLookup = providerLookup;
        this.embeddingLookup = embeddingLookup;
    }

    @Override
    public String getName() {
        return "ai-inference";
    }

    @Override
    public List<ScalarFunction> getScalarFunctions(Map<String, String> config) {
        return List.of(
                new AiCompletionFunction(providerLookup),
                new AiSummarizeFunction(providerLookup),
                new AiClassifyFunction(providerLookup),
                new AiFilterFunction(providerLookup),
                new AiEmbedFunction(embeddingLookup),
                new AiExtractFunction(providerLookup),
                new AiGuardFunction(providerLookup),
                new AiPredictFunction(providerLookup),
                new AiTranslateFunction(providerLookup),
                // Multimodal FILE functions
                new AiDescribeImageFunction(providerLookup),
                new AiParseDocumentFunction(providerLookup),
                new AiTranscribeFunction(providerLookup),
                new AiOcrFunction(providerLookup),
                new FileFromUrlFunction(),
                new FileFromBytesFunction());
    }

    private static Function<String, InferenceProvider> discoverProviderLookup() {
        ServiceLoader<InferenceProvider> loader = ServiceLoader.load(InferenceProvider.class);
        Map<String, InferenceProvider> byModel = new LinkedHashMap<>();
        InferenceProvider fallback = null;
        for (InferenceProvider p : loader) {
            byModel.put(p.modelName(), p);
            if (fallback == null) {
                fallback = p;
            }
        }
        if (fallback == null) {
            return modelName -> {
                throw new IllegalStateException(
                        "No InferenceProvider found on the classpath for model '" + modelName
                                + "'. Add an InferenceProvider implementation to the worker classpath.");
            };
        }
        InferenceProvider defaultProvider = fallback;
        return modelName -> {
            InferenceProvider p = byModel.get(modelName);
            return p != null ? p : defaultProvider;
        };
    }

    private static Function<String, EmbeddingProvider> discoverEmbeddingLookup() {
        ServiceLoader<EmbeddingProviderLookup> loader =
                ServiceLoader.load(EmbeddingProviderLookup.class);
        for (EmbeddingProviderLookup lookup : loader) {
            return lookup::resolve;
        }
        return modelName -> {
            throw new IllegalStateException(
                    "No EmbeddingProviderLookup found on the classpath for model '"
                            + modelName
                            + "'. Add an EmbeddingProviderLookup implementation "
                            + "to the worker classpath.");
        };
    }
}
