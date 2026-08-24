package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.RerankerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

/**
 * Factory for creating {@link CrossEncoderReranker} instances via ServiceLoader discovery.
 *
 * <p>Supports the following configuration properties:</p>
 * <ul>
 *   <li>{@code model} — the cross-encoder model name (required)</li>
 *   <li>{@code endpoint} — the model scoring endpoint URI (required)</li>
 *   <li>{@code timeout_ms} — request timeout in milliseconds (default 5000)</li>
 * </ul>
 */
public final class CrossEncoderRerankerFactory implements RerankerFactory {

    private static final long DEFAULT_TIMEOUT_MS = 5000;

    @Override
    public String name() {
        return "cross_encoder";
    }

    @Override
    public Reranker create(Map<String, String> properties) {
        String model = properties.get("model");
        if (model == null || model.isBlank()) {
            throw new IllegalArgumentException("cross_encoder requires 'model' property");
        }

        String endpointStr = properties.get("endpoint");
        if (endpointStr == null || endpointStr.isBlank()) {
            throw new IllegalArgumentException("cross_encoder requires 'endpoint' property");
        }
        URI endpoint = URI.create(endpointStr);

        long timeoutMs = DEFAULT_TIMEOUT_MS;
        String timeoutValue = properties.get("timeout_ms");
        if (timeoutValue != null) {
            timeoutMs = Long.parseLong(timeoutValue);
        }

        return new CrossEncoderReranker(model, endpoint, Duration.ofMillis(timeoutMs));
    }
}
