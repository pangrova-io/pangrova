package org.pangrova.planner.core.embedding;

import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.core.generation.GeneratedValueGenerationPlanBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class GeneratedColumnSourceEncodingTest {
    @Test
    void synchronousSourcesUseTheSameVersionAndSeparatorAsDurableSources() {
        var modify = GeneratedMutationFixture.plan("UPDATE documents SET title = body, body = title WHERE id = 1");
        var project = (LogicalProject) modify.getInput();
        var embedding = (RexCall) project.getProjects().get(7);
        var source = (RexCall) embedding.getOperands().get(1);
        assertThat(GeneratedValueGenerationPlanBuilder.SOURCE_ENCODING).isEqualTo(GeneratedColumnSourceEncoding.NAME);
        assertThat(RexLiteral.stringValue(source.getOperands().get(1))).isEqualTo(GeneratedColumnSourceEncoding.SEPARATOR);
        assertThat(source.getType().isNullable()).isTrue();
    }
}
