package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.RerankerFactory;

import java.util.Map;

/**
 * Factory for creating {@link RrfReranker} instances via ServiceLoader discovery.
 *
 * <p>Supports the following configuration properties:</p>
 * <ul>
 *   <li>{@code k} — the RRF k parameter (default 60). Controls how much influence
 *       lower-ranked documents have on the fused score.</li>
 * </ul>
 */
public final class RrfRerankerFactory implements RerankerFactory {

    private static final int DEFAULT_K = 60;

    @Override
    public String name() {
        return "rrf";
    }

    @Override
    public Reranker create(Map<String, String> properties) {
        int k = DEFAULT_K;
        String kValue = properties.get("k");
        if (kValue != null) {
            k = Integer.parseInt(kValue);
        }
        return new RrfReranker(k);
    }
}
