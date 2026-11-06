package org.pangrova.planner.core.property.embedding;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.pangrova.planner.core.embedding.GeneratedMutationFixture;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the actual parser/converter hook across different simultaneous NEW values. */
class GeneratedColumnMutationPropertyTest {
    @Property(tries = 30, seed = "8675309")
    void generatedSourceOrderIsIndependentOfAssignmentOrder(
            @ForAll @IntRange(min = -100, max = 100) int title,
            @ForAll @IntRange(min = -100, max = 100) int body) {
        var modify = GeneratedMutationFixture.plan("UPDATE documents SET body = 'body" + body
                + "', title = 'title" + title + "' WHERE id = 1");
        var project = (LogicalProject) modify.getInput();
        var embedding = (RexCall) project.getProjects().get(7);
        var source = (RexCall) embedding.getOperands().get(1);
        assertThat(GeneratedMutationFixture.textLiteral(source.getOperands().get(0))).isEqualTo("title" + title);
        assertThat(GeneratedMutationFixture.textLiteral(source.getOperands().get(2))).isEqualTo("body" + body);
        assertThat(modify.getUpdateColumnList()).containsExactly("body", "title", "vector");
    }
}
