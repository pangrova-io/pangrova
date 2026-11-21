package org.pangrova.observability.sdk.profiling;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QueryFlameGraphGeneratorTest {

    @Test
    void svgUsesConsistentLineFeedsAndAggregatesDuplicateStacks() {
        String svg = QueryFlameGraphGenerator.generateSvg(List.of(
                new QueryFlameGraphGenerator.OperatorSample("scan", 100),
                new QueryFlameGraphGenerator.OperatorSample("scan", 200)));
        assertThat(svg).isEqualTo("<svg xmlns=\"http://www.w3.org/2000/svg\">\n"
                + "  <rect x=\"0\" y=\"0\" width=\"3\" height=\"18\" />\n"
                + "  <text x=\"2\" y=\"14\">scan</text>\n</svg>");
    }
}
