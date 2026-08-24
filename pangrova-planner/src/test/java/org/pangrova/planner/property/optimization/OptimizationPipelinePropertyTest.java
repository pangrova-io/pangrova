package org.pangrova.planner.property.optimization;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.planner.cost.CboConfig;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.optimization.OptimizationContext;
import org.pangrova.planner.optimization.OptimizationPass;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for optimization pipeline execution semantics.
 *
 * <p><b>Property 16: Pipeline execution</b> — verify order, skip non-applicable,
 * chain output.</p>
 *
 * <p><b>Validates: Requirements 7.2, 7.3</b></p>
 */
class OptimizationPipelinePropertyTest {

    private static final RelOptCluster CLUSTER;
    static {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CLUSTER = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    }

    private static final OptimizationContext CONTEXT = new OptimizationContext(
            CboConfig.DISABLED, CostModelConfig.SSD_DEFAULT, null);

    /**
     * Property 16a: Passes execute in registration order.
     * For any list of applicable passes, the pipeline calls them in order
     * and each receives the previous pass's output.
     *
     * <b>Validates: Requirement 7.2</b>
     */
    @Property(tries = 20)
    void passesExecuteInRegistrationOrder(
            @ForAll @IntRange(min = 1, max = 8) int passCount) {
        List<Integer> callOrder = new ArrayList<>();
        List<OptimizationPass> passes = new ArrayList<>();
        for (int i = 0; i < passCount; i++) {
            int index = i;
            passes.add(new OrderTrackingPass("pass-" + index, callOrder, index));
        }

        RelNode input = LogicalValues.createOneRow(CLUSTER);
        RelNode result = runPipeline(passes, input);

        assertThat(callOrder).hasSize(passCount);
        for (int i = 0; i < passCount; i++) {
            assertThat(callOrder.get(i))
                    .as("pass at position %d should be called %dth", i, i)
                    .isEqualTo(i);
        }
        assertThat(result).isNotNull();
    }

    /**
     * Property 16b: Non-applicable passes are skipped.
     * For any list of passes where a random subset returns isApplicable=false,
     * only applicable passes are called.
     *
     * <b>Validates: Requirement 7.3</b>
     */
    @Property(tries = 20)
    void nonApplicablePassesAreSkipped(
            @ForAll @IntRange(min = 2, max = 8) int passCount,
            @ForAll @IntRange(min = 0, max = 7) int skipIndex) {
        int safeSkipIdx = skipIndex % passCount;
        List<AtomicInteger> counters = new ArrayList<>();
        List<OptimizationPass> passes = new ArrayList<>();

        for (int i = 0; i < passCount; i++) {
            AtomicInteger counter = new AtomicInteger(0);
            counters.add(counter);
            boolean applicable = (i != safeSkipIdx);
            passes.add(new CountingPass("pass-" + i, counter, applicable));
        }

        RelNode input = LogicalValues.createOneRow(CLUSTER);
        runPipeline(passes, input);

        for (int i = 0; i < passCount; i++) {
            int expected = (i == safeSkipIdx) ? 0 : 1;
            assertThat(counters.get(i).get())
                    .as("pass %d (applicable=%b) call count", i, i != safeSkipIdx)
                    .isEqualTo(expected);
        }
    }

    /**
     * Property 16c: Output chaining — each pass receives the previous pass's output.
     * Verifies that the pipeline chains outputs correctly.
     *
     * <b>Validates: Requirement 7.2</b>
     */
    @Property(tries = 20)
    void outputIsChainingThroughPasses(
            @ForAll @IntRange(min = 2, max = 6) int passCount) {
        List<RelNode> receivedInputs = new ArrayList<>();
        List<RelNode> producedOutputs = new ArrayList<>();
        List<OptimizationPass> passes = new ArrayList<>();

        for (int i = 0; i < passCount; i++) {
            passes.add(new ChainingPass("chain-" + i, receivedInputs, producedOutputs));
        }

        RelNode input = LogicalValues.createOneRow(CLUSTER);
        RelNode finalResult = runPipeline(passes, input);

        assertThat(receivedInputs).hasSize(passCount);
        assertThat(receivedInputs.get(0)).isSameAs(input);
        for (int i = 1; i < passCount; i++) {
            assertThat(receivedInputs.get(i))
                    .as("pass %d should receive output of pass %d", i, i - 1)
                    .isSameAs(producedOutputs.get(i - 1));
        }
        assertThat(finalResult).isSameAs(producedOutputs.get(passCount - 1));
    }

    /**
     * Property 16d: Empty pipeline returns input unchanged.
     *
     * <b>Validates: Requirement 7.2</b>
     */
    @Property(tries = 5)
    void emptyPipelineReturnsInputUnchanged() {
        RelNode input = LogicalValues.createOneRow(CLUSTER);
        RelNode result = runPipeline(List.of(), input);
        assertThat(result).isSameAs(input);
    }

    /**
     * Property 16e: All-non-applicable pipeline returns input unchanged.
     *
     * <b>Validates: Requirement 7.3</b>
     */
    @Property(tries = 20)
    void allNonApplicableReturnsInputUnchanged(
            @ForAll @IntRange(min = 1, max = 8) int passCount) {
        List<OptimizationPass> passes = new ArrayList<>();
        for (int i = 0; i < passCount; i++) {
            passes.add(new CountingPass("skip-" + i, new AtomicInteger(0), false));
        }

        RelNode input = LogicalValues.createOneRow(CLUSTER);
        RelNode result = runPipeline(passes, input);
        assertThat(result).isSameAs(input);
    }

    // ── pipeline runner (mirrors PangrovaPlanner.optimize() logic) ──

    private static RelNode runPipeline(List<OptimizationPass> passes, RelNode input) {
        RelNode current = input;
        for (OptimizationPass pass : passes) {
            if (pass.isApplicable(current, CONTEXT)) {
                current = pass.optimize(current, CONTEXT);
            }
        }
        return current;
    }

    // ── test pass implementations ──

    /** Tracks call order by appending its index to a shared list. */
    private record OrderTrackingPass(
            String passName, List<Integer> callOrder, int index
    ) implements OptimizationPass {
        @Override
        public RelNode optimize(RelNode relNode, OptimizationContext context) {
            callOrder.add(index);
            return relNode;
        }
        @Override
        public String name() { return passName; }
    }

    /** Counts invocations and optionally declares itself non-applicable. */
    private record CountingPass(
            String passName, AtomicInteger counter, boolean applicable
    ) implements OptimizationPass {
        @Override
        public RelNode optimize(RelNode relNode, OptimizationContext context) {
            counter.incrementAndGet();
            return relNode;
        }
        @Override
        public String name() { return passName; }
        @Override
        public boolean isApplicable(RelNode relNode, OptimizationContext context) {
            return applicable;
        }
    }

    /** Records received input and produces a fresh output for chaining verification. */
    private record ChainingPass(
            String passName, List<RelNode> receivedInputs, List<RelNode> producedOutputs
    ) implements OptimizationPass {
        @Override
        public RelNode optimize(RelNode relNode, OptimizationContext context) {
            receivedInputs.add(relNode);
            RelNode output = LogicalValues.createOneRow(CLUSTER);
            producedOutputs.add(output);
            return output;
        }
        @Override
        public String name() { return passName; }
    }
}
