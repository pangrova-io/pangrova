package org.pangrova.planner;

import com.google.common.collect.ImmutableList;
import io.substrait.proto.Plan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.planner.cost.distribution.PangrovaExchangeInput;
import org.pangrova.planner.cost.join.BroadcastJoinConfig;
import org.pangrova.planner.cost.join.PangrovaBroadcastJoin;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;
import org.pangrova.planner.validation.NativeCapabilityRegistry;
import org.pangrova.planner.validation.SubstraitPlanValidator;
import org.pangrova.planner.validation.ValidationStatus;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PlanFragmenterTest {

    @Test
    void unknownDeclaredSourceDoesNotTreatCalciteDefault100AsObservedRows() {
        // This is a Calcite metadata-only unit: no connector or row execution is substituted.
        var schema = org.apache.calcite.tools.Frameworks.createRootSchema(true);
        schema.add("declared", new org.apache.calcite.schema.impl.AbstractTable() {
            @Override public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("id", SqlTypeName.BIGINT).build();
            }
        });
        var builder = org.apache.calcite.tools.RelBuilder.create(
                org.apache.calcite.tools.Frameworks.newConfigBuilder().defaultSchema(schema).build());
        RelNode scan = builder.scan("declared").build();
        assertThat(scan.getCluster().getMetadataQuery().getRowCount(scan)).isEqualTo(100.0);
        var stages = PlanFragmenter.fragment(scan, 0,
                AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT);
        assertThat(stages.stages()).singleElement().satisfies(stage -> assertThat(stage.parallelism()).isEqualTo(4));
    }

    private RelOptCluster cluster;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    void setUp() {
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        VolcanoPlanner planner = new VolcanoPlanner();
        planner.setExecutor(RexUtil.EXECUTOR);
        cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
    }

    @Test
    void fragmentsExchangeIntoBoundProducerAndTypedConsumer() {
        FragmentedPlan fragmented = PlanFragmenter.fragment(hashExchange(), 8);

        StageTransfer hashTransfer = fragmented.transfers().stream()
                .filter(transfer -> transfer.shuffleType() == ShuffleType.HASH)
                .findFirst()
                .orElseThrow();
        PlanStage producer = stage(fragmented, hashTransfer.upstreamStageId());
        PlanStage consumer = stage(fragmented, hashTransfer.downstreamStageId());

        assertThat(hashTransfer.exchangeId()).isEqualTo("exchange-1-to-0");
        assertThat(hashTransfer.partitionCount()).isEqualTo(8);
        assertThat(hashTransfer.batchSerde()).isEqualTo(BatchSerde.BATCH_SERDE_ARROW_BATCH);
        assertThat(hashTransfer.requireComplete()).isTrue();
        assertThat(fragmented.stageDependencies().get(consumer.stageId()))
                .containsExactly(producer.stageId());

        assertThat(producer.fragment().relNode()).isInstanceOf(PangrovaExchange.class);
        PangrovaExchange producerExchange =
                (PangrovaExchange) producer.fragment().relNode();
        assertThat(producerExchange.partitionCount()).isEqualTo(8);
        assertThat(producerExchange.getInput()).isInstanceOf(LogicalValues.class);

        PangrovaExchangeInput input = findExchangeInput(consumer.fragment().relNode());
        assertThat(input.exchangeId()).isEqualTo(hashTransfer.exchangeId());
        assertThat(input.upstreamStageId()).isEqualTo(producer.stageId());
        assertThat(input.downstreamStageId()).isEqualTo(consumer.stageId());
        assertThat(input.partitionCount()).isEqualTo(8);
        assertThat(input.requireComplete()).isTrue();
    }

    @Test
    void broadcastJoinExchangeIsVisibleAndBoundBeforeSerialization() throws Exception {
        RelNode left = values();
        RelNode right = values();
        PangrovaExchange broadcast = new PangrovaExchange(
                cluster,
                right.getTraitSet().replace(PangrovaDistribution.BROADCAST),
                right,
                PangrovaDistribution.BROADCAST);
        var condition = cluster.getRexBuilder().makeCall(
                SqlStdOperatorTable.EQUALS,
                cluster.getRexBuilder().makeInputRef(left, 0),
                cluster.getRexBuilder().makeInputRef(right.getRowType()
                        .getFieldList().get(0).getType(), left.getRowType().getFieldCount()));
        PangrovaBroadcastJoin join = new PangrovaBroadcastJoin(
                new BroadcastJoinConfig(cluster, left.getTraitSet(), left,
                        broadcast, condition, Set.of(), JoinRelType.INNER));

        FragmentedPlan fragmented = PlanFragmenter.fragment(join, -1);
        StageTransfer transfer = fragmented.transfers().stream()
                .filter(candidate -> candidate.shuffleType() == ShuffleType.BROADCAST)
                .findFirst().orElseThrow();
        PlanStage producer = stage(fragmented, transfer.upstreamStageId());
        PlanStage consumer = stage(fragmented, transfer.downstreamStageId());

        assertThat(fragmented.stages()).hasSize(2);
        assertThat(consumer.fragment().relNode())
                .isInstanceOf(PangrovaBroadcastJoin.class);
        assertThat(((PangrovaBroadcastJoin) consumer.fragment().relNode()).getRight())
                .isInstanceOf(PangrovaExchangeInput.class);
        assertThat(producer.fragment().relNode()).isInstanceOf(PangrovaExchange.class);
        assertThat(serialize(producer.fragment().relNode()).getExchange().getPartitionCount())
                .isEqualTo(consumer.parallelism());
    }

    @Test
    void serializesProducerAndConsumerThroughCanonicalProtocol() throws Exception {
        FragmentedPlan fragmented = PlanFragmenter.fragment(hashExchange(), 8);
        StageTransfer transfer = fragmented.transfers().stream()
                .filter(candidate -> candidate.shuffleType() == ShuffleType.HASH)
                .findFirst()
                .orElseThrow();
        PlanStage producer = stage(fragmented, transfer.upstreamStageId());
        PlanStage consumer = stage(fragmented, transfer.downstreamStageId());

        var validator = new SubstraitPlanValidator(NativeCapabilityRegistry.defaults());
        assertThat(validator.validate(new SubstraitConverter().convert(
                producer.fragment().relNode())).status())
                .isEqualTo(ValidationStatus.FULLY_NATIVE);
        assertThat(validator.validate(new SubstraitConverter().convert(
                consumer.fragment().relNode())).status())
                .isEqualTo(ValidationStatus.FULLY_NATIVE);

        io.substrait.proto.Rel producerRel = serialize(producer.fragment().relNode());
        assertThat(producerRel.hasExchange()).isTrue();
        assertThat(producerRel.getExchange().getPartitionCount()).isEqualTo(8);
        assertThat(producerRel.getExchange().hasScatterByFields()).isTrue();

        io.substrait.proto.Rel consumerRel = serialize(consumer.fragment().relNode());
        assertThat(consumerRel.hasExchange()).isTrue();
        io.substrait.proto.Rel inputRel = consumerRel.getExchange().getInput();
        assertThat(inputRel.hasExtensionLeaf()).isTrue();
        RelationDetailEnvelope envelope = inputRel.getExtensionLeaf().getDetail()
                .unpack(RelationDetailEnvelope.class);
        assertThat(envelope.hasExchangeInput()).isTrue();
        assertThat(envelope.getExchangeInput().getExchangeId())
                .isEqualTo(transfer.exchangeId());
        assertThat(envelope.getExchangeInput().getPartitionCount()).isEqualTo(8);
        assertThat(envelope.getExchangeInput().getBatchSerde())
                .isEqualTo(BatchSerde.BATCH_SERDE_ARROW_BATCH);
    }

    @Test
    void addsSingletonOutputStageForParallelRoot() {
        FragmentedPlan fragmented = PlanFragmenter.fragment(hashExchange(), 8);

        PlanStage output = fragmented.stages().stream()
                .filter(stage -> "OUTPUT".equals(stage.taskType()))
                .findFirst()
                .orElseThrow();
        StageTransfer gather = fragmented.transfers().stream()
                .filter(transfer -> transfer.downstreamStageId() == output.stageId())
                .findFirst()
                .orElseThrow();

        assertThat(output.parallelism()).isOne();
        assertThat(gather.shuffleType()).isEqualTo(ShuffleType.SINGLETON);
        assertThat(gather.partitionCount()).isOne();
        assertThat(output.fragment().relNode()).isInstanceOf(PangrovaExchangeInput.class);
    }

    @Test
    void relationalExportKeepsParallelTerminalWithoutQueryResultGather() {
        var graph = PlanFragmenter.fragment(hashExchange(), 4,
                AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT);
        var output = graph.stages().stream().filter(stage -> "OUTPUT".equals(stage.taskType()))
                .findFirst().orElseThrow();
        assertThat(output.parallelism()).isEqualTo(4);
        assertThat(graph.transfers()).hasSize(1);
        assertThat(graph.transfers().get(0).shuffleType()).isEqualTo(ShuffleType.HASH);
        assertThat(graph.transfers()).noneMatch(edge -> edge.shuffleType() == ShuffleType.SINGLETON);
    }

    @Test
    void relationalExportStillExecutesTopKOverTheCompleteStream() {
        var topK = LogicalSort.create(hashExchange(), RelCollations.of(new RelFieldCollation(0)),
                null, cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(2)));
        var graph = PlanFragmenter.fragment(topK, 4, AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT);
        var output = graph.stages().stream().filter(stage -> "OUTPUT".equals(stage.taskType()))
                .findFirst().orElseThrow();
        assertThat(output.parallelism()).isOne();
        assertThat(output.fragment().relNode()).isInstanceOf(LogicalSort.class);
        assertThat(((LogicalSort) output.fragment().relNode()).getInput()).isInstanceOf(PangrovaExchangeInput.class);
        assertThat(graph.transfers()).anyMatch(edge -> edge.shuffleType() == ShuffleType.SINGLETON
                && edge.downstreamStageId() == output.stageId());
    }

    @Test
    void reappliesTopKAboveSingletonExchangeForGlobalCorrectness() {
        PangrovaDistribution distribution = PangrovaDistribution.hash(List.of(0));
        PangrovaExchange source = new PangrovaExchange(
                cluster, cluster.traitSet().replace(distribution),
                retrievalValues(), distribution);
        LogicalSort topK = LogicalSort.create(
                source,
                RelCollations.of(new RelFieldCollation(
                        1, RelFieldCollation.Direction.ASCENDING)),
                null,
                cluster.getRexBuilder().makeExactLiteral(BigDecimal.valueOf(2)));

        FragmentedPlan fragmented = PlanFragmenter.fragment(topK, 8);

        PlanStage output = fragmented.stages().stream()
                .filter(stage -> "OUTPUT".equals(stage.taskType()))
                .findFirst()
                .orElseThrow();
        assertThat(output.parallelism()).isOne();
        assertThat(output.fragment().relNode()).isInstanceOf(LogicalSort.class);
        LogicalSort finalTopK = (LogicalSort) output.fragment().relNode();
        assertThat(finalTopK.getInput()).isInstanceOf(PangrovaExchangeInput.class);
        assertThat(RexLiteral.intValue(finalTopK.fetch)).isEqualTo(2);

        var conversion = new SubstraitConverter().convertWithResult(
                output.fragment().relNode(), List.of());
        assertThat(conversion.retrievalTopKHints()).hasSize(1);
        assertThat(conversion.retrievalTopKHints().get(0).candidateLimit()).isEqualTo(2);
    }

    @Test
    void hashPartitionsRawRowsBeforeGroupedNativeAggregation() {
        LogicalAggregate aggregate = LogicalAggregate.create(
                roundRobinExchange(), List.of(), ImmutableBitSet.of(0), null, List.of());

        FragmentedPlan fragmented = PlanFragmenter.fragment(aggregate, 4);

        StageTransfer hash = fragmented.transfers().stream()
                .filter(transfer -> transfer.shuffleType() == ShuffleType.HASH)
                .findFirst().orElseThrow();
        assertThat(hash.distributionKeys()).containsExactly(0);
        assertThat(hash.partitionCount()).isEqualTo(4);
        RelNode consumer = stage(fragmented, hash.downstreamStageId()).fragment().relNode();
        if (consumer instanceof PangrovaExchange exchange) {
            consumer = exchange.getInput();
        }
        assertThat(consumer).isInstanceOf(LogicalAggregate.class);
        assertThat(((LogicalAggregate) consumer).getInput())
                .isInstanceOf(PangrovaExchangeInput.class);
    }

    @Test
    void gathersRawRowsBeforeGlobalNativeAggregation() {
        LogicalAggregate aggregate = LogicalAggregate.create(
                roundRobinExchange(), List.of(), ImmutableBitSet.of(), null, List.of());

        FragmentedPlan fragmented = PlanFragmenter.fragment(aggregate, 4);

        StageTransfer gather = fragmented.transfers().stream()
                .filter(transfer -> transfer.shuffleType() == ShuffleType.SINGLETON)
                .findFirst().orElseThrow();
        PlanStage consumer = stage(fragmented, gather.downstreamStageId());
        assertThat(consumer.parallelism()).isOne();
        assertThat(consumer.fragment().relNode()).isInstanceOf(LogicalAggregate.class);
        assertThat(((LogicalAggregate) consumer.fragment().relNode()).getInput())
                .isInstanceOf(PangrovaExchangeInput.class);
    }

    @Test
    void sortsCompleteStreamOnOneNativeWorker() {
        LogicalSort sort = LogicalSort.create(
                hashExchange(),
                RelCollations.of(new RelFieldCollation(0)), null, null);

        FragmentedPlan fragmented = PlanFragmenter.fragment(sort, 4);

        PlanStage output = fragmented.stages().stream()
                .filter(stage -> "OUTPUT".equals(stage.taskType()))
                .findFirst().orElseThrow();
        assertThat(output.parallelism()).isOne();
        assertThat(output.fragment().relNode()).isInstanceOf(LogicalSort.class);
        assertThat(((LogicalSort) output.fragment().relNode()).getInput())
                .isInstanceOf(PangrovaExchangeInput.class);
    }

    @Test
    void hashPartitionsCompleteWindowGroupsBeforeNativeEvaluation() {
        FragmentedPlan fragmented = PlanFragmenter.fragment(window(ImmutableBitSet.of(0)), 4);
        StageTransfer hash = fragmented.transfers().stream()
                .filter(edge -> edge.shuffleType() == ShuffleType.HASH).findFirst().orElseThrow();
        assertThat(hash.distributionKeys()).containsExactly(0);
        assertThat(hash.partitionCount()).isEqualTo(4);
        RelNode consumer = stage(fragmented, hash.downstreamStageId()).fragment().relNode();
        if (consumer instanceof PangrovaExchange exchange) consumer = exchange.getInput();
        assertThat(consumer).isInstanceOf(LogicalWindow.class);
        assertThat(((LogicalWindow) consumer).getInput()).isInstanceOf(PangrovaExchangeInput.class);
    }

    @Test
    void gathersUnpartitionedWindowWithoutSerializingSourceScan() {
        FragmentedPlan fragmented = PlanFragmenter.fragment(window(ImmutableBitSet.of()), 4);
        StageTransfer gather = fragmented.transfers().stream()
                .filter(edge -> edge.shuffleType() == ShuffleType.SINGLETON).findFirst().orElseThrow();
        var consumer = stage(fragmented, gather.downstreamStageId());
        assertThat(consumer.parallelism()).isOne();
        assertThat(consumer.fragment().relNode()).isInstanceOf(LogicalWindow.class);
        assertThat(((LogicalWindow) consumer.fragment().relNode()).getInput())
                .isInstanceOf(PangrovaExchangeInput.class);
        assertThat(stage(fragmented, gather.upstreamStageId()).parallelism()).isEqualTo(4);
    }

    private LogicalWindow window(ImmutableBitSet keys) {
        RelNode input = roundRobinExchange();
        RelDataType resultType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        var group = new Window.Group(keys, true, RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.CURRENT_ROW, RexWindowExclusion.EXCLUDE_NO_OTHER,
                RelCollations.of(new RelFieldCollation(0)), List.of(new Window.RexWinAggCall(
                        SqlStdOperatorTable.ROW_NUMBER, resultType, List.of(), 0, false)));
        var rowType = typeFactory.builder().addAll(input.getRowType().getFieldList())
                .add("row_number", resultType).build();
        return LogicalWindow.create(input.getTraitSet(), List.of(), input, List.of(), rowType, List.of(group));
    }

    @Test
    void rejectsUnboundHashDistribution() {
        PangrovaExchange exchange = new PangrovaExchange(
                cluster,
                cluster.traitSet(),
                values(),
                PangrovaDistribution.HASH);

        assertThatThrownBy(() -> PlanFragmenter.fragment(exchange, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition key");
    }

    private PangrovaExchange hashExchange() {
        PangrovaDistribution distribution = PangrovaDistribution.hash(List.of(0));
        return new PangrovaExchange(
                cluster,
                cluster.traitSet().replace(distribution),
                values(),
                distribution);
    }

    private PangrovaExchange roundRobinExchange() {
        return new PangrovaExchange(
                cluster,
                cluster.traitSet().replace(PangrovaDistribution.ROUND_ROBIN),
                values(),
                PangrovaDistribution.ROUND_ROBIN);
    }

    private LogicalValues values() {
        RelDataType rowType = typeFactory.builder()
                .add("id", SqlTypeName.INTEGER)
                .add("text", SqlTypeName.VARCHAR, 32)
                .build();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        ImmutableList<ImmutableList<RexLiteral>> tuples = ImmutableList.of(
                ImmutableList.of(
                        rexBuilder.makeExactLiteral(
                                BigDecimal.ONE,
                                typeFactory.createSqlType(SqlTypeName.INTEGER)),
                        rexBuilder.makeLiteral("row")));
        return LogicalValues.create(cluster, rowType, tuples);
    }

    private LogicalValues retrievalValues() {
        RelDataType rowType = typeFactory.builder()
                .add("id", SqlTypeName.BIGINT)
                .add("score", SqlTypeName.DOUBLE)
                .build();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        ImmutableList<ImmutableList<RexLiteral>> tuples = ImmutableList.of(
                ImmutableList.of(
                        rexBuilder.makeExactLiteral(BigDecimal.ONE,
                                typeFactory.createSqlType(SqlTypeName.BIGINT)),
                        rexBuilder.makeApproxLiteral(BigDecimal.valueOf(0.25),
                                typeFactory.createSqlType(SqlTypeName.DOUBLE))));
        return LogicalValues.create(cluster, rowType, tuples);
    }

    private static PlanStage stage(FragmentedPlan fragmented, int stageId) {
        return fragmented.stages().stream()
                .filter(stage -> stage.stageId() == stageId)
                .findFirst()
                .orElseThrow();
    }

    private static PangrovaExchangeInput findExchangeInput(RelNode node) {
        if (node instanceof PangrovaExchangeInput input) {
            return input;
        }
        return node.getInputs().stream()
                .map(PlanFragmenterTest::findExchangeInput)
                .findFirst()
                .orElseThrow();
    }

    private static io.substrait.proto.Rel serialize(RelNode node) throws Exception {
        byte[] bytes = new SubstraitPlanSerializer().serialize(
                new SubstraitConverter().convert(node));
        return Plan.parseFrom(bytes).getRelations(0).getRoot().getInput();
    }
}
