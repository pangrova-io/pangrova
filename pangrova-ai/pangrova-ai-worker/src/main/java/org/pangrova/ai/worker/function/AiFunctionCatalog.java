package org.pangrova.ai.worker.function;

import org.pangrova.ai.spi.embedding.EmbeddingProvider;
import org.pangrova.ai.spi.inference.InferenceProvider;
import org.pangrova.ai.spi.reranking.RerankingProvider;
import org.pangrova.function.spi.scalar.ScalarFunction;
import org.pangrova.ai.worker.blob.GovernedBlobStore;
import org.pangrova.ai.worker.blob.HttpsBlobImporter;
import org.pangrova.ai.worker.function.file.FileFromBytesFunction;
import org.pangrova.ai.worker.function.file.FileFromUrlFunction;

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
            Throwable startupFailure, AutoCloseable... resources) {
        for (AutoCloseable resource : resources) {
            if (resource == null) {
                continue;
            }
            try {
                resource.close();
            } catch (Exception | Error closeFailure) {
                if (startupFailure != closeFailure) startupFailure.addSuppressed(closeFailure);
            }
        }
    }

    private static ScalarFunction governed(ScalarFunction function) {
        return new GovernedAiScalarFunction(function);
    }
}
