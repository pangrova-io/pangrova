package org.pangrova.observability.profiling;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Aggregates per-operator CPU/wall-time into flame graph data and generates SVG output.
 */
public final class QueryFlameGraphGenerator {

    private QueryFlameGraphGenerator() {}

    /**
     * Generates an SVG flame graph from operator-level profiling samples.
     */
    public static String generateSvg(List<OperatorSample> samples) {
        Map<String, Long> aggregated = aggregateSamples(samples);
        return renderSvg(aggregated);
    }

    private static Map<String, Long> aggregateSamples(List<OperatorSample> samples) {
        return samples.stream()
                .collect(Collectors.groupingBy(
                        OperatorSample::stackTrace,
                        Collectors.summingLong(OperatorSample::durationMicros)));
    }

    private static String renderSvg(Map<String, Long> aggregated) {
        StringBuilder svg = new StringBuilder();
        svg.append("<svg xmlns=\"http://www.w3.org/2000/svg\">\n");
        int y = 0;
        for (Map.Entry<String, Long> entry : aggregated.entrySet()) {
            int width = Math.max(1, (int) (entry.getValue() / 100));
            appendRect(svg, entry.getKey(), width, y);
            y += 20;
        }
        svg.append("</svg>");
        return svg.toString();
    }

    private static void appendRect(StringBuilder svg, String label,
                                    int width, int y) {
        svg.append(String.format(
                "  <rect x=\"0\" y=\"%d\" width=\"%d\" height=\"18\" />", y, width)).append('\n');
        svg.append(String.format(
                "  <text x=\"2\" y=\"%d\">%s</text>", y + 14, label)).append('\n');
    }

    /**
     * A single profiling sample for an operator in the query execution tree.
     */
    public record OperatorSample(String stackTrace, long durationMicros) {}
}
