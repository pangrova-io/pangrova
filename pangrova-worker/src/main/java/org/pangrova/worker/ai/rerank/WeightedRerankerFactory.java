package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.RerankerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating {@link WeightedReranker} instances via ServiceLoader discovery.
 *
 * <p>Supports weight configuration via properties with the prefix {@code weight.}
 * followed by the source name. For example:</p>
 * <ul>
 *   <li>{@code weight.vector=0.7} — assigns 70% weight to the "vector" source</li>
 *   <li>{@code weight.fts=0.3} — assigns 30% weight to the "fts" source</li>
 * </ul>
 *
 * <p>If no weight properties are provided, a default equal-weight configuration
 * with sources "vector" and "fts" is used.</p>
 */
public final class WeightedRerankerFactory implements RerankerFactory {

    private static final String WEIGHT_PREFIX = "weight.";
    private static final double DEFAULT_SOURCE_WEIGHT = 0.5;

    @Override
    public String name() {
        return "weighted";
    }

    @Override
    public Reranker create(Map<String, String> properties) {
        Map<String, Double> weights = new HashMap<>();

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(WEIGHT_PREFIX)) {
                String source = entry.getKey().substring(WEIGHT_PREFIX.length());
                weights.put(source, Double.parseDouble(entry.getValue()));
            }
        }

        if (weights.isEmpty()) {
            weights.put("vector", DEFAULT_SOURCE_WEIGHT);
            weights.put("fts", DEFAULT_SOURCE_WEIGHT);
        }

        return new WeightedReranker(weights);
    }
}
