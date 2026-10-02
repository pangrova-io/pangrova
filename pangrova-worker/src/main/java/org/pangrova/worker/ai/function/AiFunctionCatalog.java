package org.pangrova.worker.ai.function;

import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.spi.ai.inference.InferenceProvider;
import org.pangrova.spi.ai.rerank.RerankingProvider;
import org.pangrova.spi.function.ScalarFunction;
import org.pangrova.worker.ai.blob.GovernedBlobStore;
import org.pangrova.worker.ai.blob.HttpsBlobImporter;
import org.pangrova.worker.ai.function.file.FileFromBytesFunction;
import org.pangrova.worker.ai.function.file.FileFromUrlFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Builds the exact scalar-function set for enabled Worker AI and FILE runtimes. */
final class AiFunctionCatalog {

    private AiFunctionCatalog() {}

    static List<ScalarFunction> aiFunctions(
            Function<String, InferenceProvider> inference,
            Function<String, EmbeddingProvider> embedding,
            Function<String, RerankingProvider> reranking) {
        return List.of(
                governed(new AiCompletionFunction(inference)),
                governed(new AiSummarizeFunction(inference)),
                governed(new AiClassifyFunction(inference)),
                governed(new AiFilterFunction(inference)),
                governed(new AiEmbedFunction(embedding)),
                governed(new AiExtractFunction(inference)),
                governed(new AiGuardFunction(inference)),
                governed(new AiPredictFunction(inference)),
                governed(new AiTranslateFunction(inference)),
                governed(new AiDescribeImageFunction(inference)),
                governed(new AiParseDocumentFunction(inference)),
                governed(new AiTranscribeFunction(inference)),
                governed(new AiOcrFunction(inference)),
                new AiRerankScoreFunction(reranking));
    }

    static List<ScalarFunction> fileFunctions(
            GovernedBlobStore store, HttpsBlobImporter importer) {
        return List.of(
                new FileFromBytesFunction(store),
                new FileFromUrlFunction(importer));
    }

    static List<ScalarFunction> combine(
            List<ScalarFunction> aiFunctions,
            List<ScalarFunction> fileFunctions) {
        List<ScalarFunction> result = new ArrayList<>(aiFunctions);
        result.addAll(fileFunctions);
        return List.copyOf(result);
    }

    static void closeAfterFailure(
            RuntimeException startupFailure, AutoCloseable... resources) {
        for (AutoCloseable resource : resources) {
            if (resource == null) {
                continue;
            }
            try {
                resource.close();
            } catch (Exception closeFailure) {
                startupFailure.addSuppressed(closeFailure);
            }
        }
    }

    private static ScalarFunction governed(ScalarFunction function) {
        return new GovernedAiScalarFunction(function);
    }
}
