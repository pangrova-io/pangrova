package org.pangrova.worker.ai.rerank;

import org.pangrova.spi.ai.rerank.Reranker;
import org.pangrova.spi.ai.rerank.RerankerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Factory for creating {@link CascadeReranker} instances via ServiceLoader discovery.
 *
 * <p>Supports the following configuration properties:</p>
 * <ul>
 *   <li>{@code stages} — comma-separated list of reranker names to chain (required).
 *       Example: "rrf,cross_encoder"</li>
 *   <li>Stage-specific properties are prefixed with the stage name and a dot.
 *       Example: "rrf.k=60", "cross_encoder.model=ms_marco"</li>
 * </ul>
 */
public final class CascadeRerankerFactory implements RerankerFactory {

    @Override
    public String name() {
        return "cascade";
    }

    @Override
    public Reranker create(Map<String, String> properties) {
        String stagesValue = properties.get("stages");
        if (stagesValue == null || stagesValue.isBlank()) {
            throw new IllegalArgumentException("cascade requires 'stages' property (comma-separated reranker names)");
        }

        String[] stageNames = stagesValue.split(",");
        List<Reranker> stages = new ArrayList<>(stageNames.length);

        for (String stageName : stageNames) {
            String trimmed = stageName.trim();
            Map<String, String> stageProperties = extractStageProperties(trimmed, properties);
            RerankerFactory factory = findFactory(trimmed);
            stages.add(factory.create(stageProperties));
        }

        return new CascadeReranker(stages);
    }

    private Map<String, String> extractStageProperties(String stageName, Map<String, String> allProperties) {
        String prefix = stageName + ".";
        Map<String, String> stageProps = new HashMap<>();
        for (Map.Entry<String, String> entry : allProperties.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                stageProps.put(entry.getKey().substring(prefix.length()), entry.getValue());
            }
        }
        return stageProps;
    }

    private RerankerFactory findFactory(String name) {
        ServiceLoader<RerankerFactory> loader = ServiceLoader.load(RerankerFactory.class);
        for (RerankerFactory factory : loader) {
            if (factory.name().equals(name) && !factory.name().equals("cascade")) {
                return factory;
            }
        }
        throw new IllegalArgumentException("Reranker factory not found for stage: " + name);
    }
}
