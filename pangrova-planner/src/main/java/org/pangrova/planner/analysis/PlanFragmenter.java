/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.planner.analysis;

import org.pangrova.planner.exchange.AggregateExchangeBinding;
import org.pangrova.planner.exchange.DecimalAggregateExchange;
import org.pangrova.planner.exchange.StageAggregate;
import org.pangrova.planner.enums.DistributionType;
import org.pangrova.planner.enums.StageTerminalPurpose;
import org.pangrova.planner.enums.ShuffleType;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.pangrova.planner.cost.distribution.PangrovaDistribution;
import org.pangrova.planner.cost.distribution.PangrovaExchange;
import org.pangrova.planner.cost.distribution.PangrovaExchangeInput;
import org.pangrova.planner.cost.distribution.WindowPartitioning;
import org.pangrova.planner.cost.join.PangrovaBroadcastJoin;
import org.pangrova.planner.cost.join.BroadcastJoinConfig;
import org.pangrova.planner.cost.join.PangrovaHashJoin;
import org.pangrova.planner.metadata.PlanTable;
import org.pangrova.planner.metadata.catalog.MetadataResolvedTable;
import io.substrait.proto.AggregationPhase;
import org.pangrova.protocol.function.catalog.FunctionCatalogView;
import org.pangrova.protocol.v2.BatchSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Splits a physical plan into self-contained producer and consumer fragments.
 * A producer terminates in a standard Substrait exchange and a consumer starts
 * at a typed exchange-input leaf. Runtime endpoints never enter plan bytes.
 */
final class PlanFragmenter {

    private static final Logger LOG = LoggerFactory.getLogger(PlanFragmenter.class);
    private static final int DEFAULT_PARALLELISM = 4;
    private static final int SINGLE_PARALLELISM = 1;
    private static final long DEFAULT_BYTES_PER_PARTITION = 256L * 1024 * 1024;
    private static final int DEFAULT_MAX_PARALLELISM = 128;
    private static final BatchSerde EXCHANGE_SERDE = BatchSerde.BATCH_SERDE_ARROW_BATCH;

    private PlanFragmenter() {}

    static FragmentedPlan fragment(RelNode root, int sessionParallelismOverride) {
        return fragment(root, sessionParallelismOverride, StageTerminalPurpose.QUERY_RESULT);
    }

    static FragmentedPlan fragment(RelNode root, int sessionParallelismOverride,
            StageTerminalPurpose terminalPurpose) {
        return fragment(root, sessionParallelismOverride, terminalPurpose,
                DEFAULT_MAX_PARALLELISM);
    }

    static FragmentedPlan fragment(RelNode root, int sessionParallelismOverride,
            StageTerminalPurpose terminalPurpose, int clusterParallelismLimit) {
        return fragment(root, sessionParallelismOverride, terminalPurpose, clusterParallelismLimit, false);
    }

    static FragmentedPlan fragment(RelNode root, int sessionParallelismOverride,
            StageTerminalPurpose terminalPurpose, int clusterParallelismLimit, boolean needsRetrievalReducer) {
        return fragment(root, sessionParallelismOverride, terminalPurpose, clusterParallelismLimit,
                needsRetrievalReducer, List::of, ignored -> 0);
    }

    static FragmentedPlan fragment(RelNode root, int sessionParallelismOverride,
            StageTerminalPurpose terminalPurpose, int clusterParallelismLimit, boolean needsRetrievalReducer,
            Supplier<List<FunctionCatalogView>> catalogueSource, IntUnaryOperator driverEstimate) {
        checkNotNull(root, "root RelNode must not be null");
        if (clusterParallelismLimit < 1) {
            throw new IllegalArgumentException("Cluster parallelism limit must be positive");
        }
        FragmentContext context = new FragmentContext(
                sessionParallelismOverride,
                DEFAULT_BYTES_PER_PARTITION,
                Math.min(DEFAULT_MAX_PARALLELISM, clusterParallelismLimit),
                checkNotNull(terminalPurpose, "terminalPurpose must not be null"), catalogueSource, driverEstimate);
        return context.fragment(root, needsRetrievalReducer);
    }

    static ShuffleType toShuffleType(RelDistribution.Type distributionType) {
        return switch (distributionType) {
            case BROADCAST_DISTRIBUTED -> ShuffleType.BROADCAST;
            case HASH_DISTRIBUTED -> ShuffleType.HASH;
            case RANGE_DISTRIBUTED -> ShuffleType.RANGE;
            case RANDOM_DISTRIBUTED, ROUND_ROBIN_DISTRIBUTED -> ShuffleType.ROUND_ROBIN;
            case SINGLETON -> ShuffleType.SINGLETON;
            case ANY -> ShuffleType.ANY;
        };
    }

    private static ShuffleType toShuffleType(PangrovaDistribution distribution) {
        return switch (distribution.type()) {
            case HASH -> ShuffleType.HASH;
            case BROADCAST -> ShuffleType.BROADCAST;
            case ROUND_ROBIN -> ShuffleType.ROUND_ROBIN;
            case SINGLETON -> ShuffleType.SINGLETON;
            case ANY -> ShuffleType.ANY;
        };
    }

    private static final class FragmentContext {

        private final Map<Integer, MutableStage> stages = new HashMap<>();
        private final Map<Integer, List<Integer>> dependencies = new HashMap<>();
        private final List<StageTransfer> transfers = new ArrayList<>();
        private final List<StageRuntimeFilter> runtimeFilters = new ArrayList<>(1);
        private final int sessionParallelismOverride;
        private final long bytesPerPartition;
        private final int maxParallelism;
        private final StageTerminalPurpose terminalPurpose;
        private final Supplier<List<FunctionCatalogView>> catalogueSource;
        private List<FunctionCatalogView> functionCatalogues;
        private final IntUnaryOperator driverEstimate;
        private int nextStageId;
        private boolean liveRead;

        private FragmentContext(
                int sessionParallelismOverride,
                long bytesPerPartition,
                int maxParallelism,
                StageTerminalPurpose terminalPurpose,
                Supplier<List<FunctionCatalogView>> catalogueSource,
                IntUnaryOperator driverEstimate) {
            this.catalogueSource = catalogueSource;
            this.driverEstimate = driverEstimate;
            this.sessionParallelismOverride = sessionParallelismOverride;
            this.bytesPerPartition = bytesPerPartition;
            this.maxParallelism = maxParallelism;
            this.terminalPurpose = terminalPurpose;
        }

        FragmentedPlan fragment(RelNode root, boolean needsRetrievalReducer) {
            // A correlated subtree cannot be split before its outer-row
            // binding has been turned into an ordinary join condition.
            boolean correlatedExecution = containsCorrelatedExecution(root);
            liveRead = !correlatedExecution && !(root instanceof TableModify) && !needsRetrievalReducer
                    && terminalPurpose == StageTerminalPurpose.QUERY_RESULT;
            if (!correlatedExecution) {
                root = enforceGlobalOperators(root, true);
            }
            // A TableModify emits rows for one fenced WRITE task, even though
            // Calcite describes its logical result as a single row count. An
            // exchange above it would carry the wrong schema and create an
            // additional writer for the same commit binding.
            int parallelism = correlatedExecution || root instanceof TableModify
                    ? SINGLE_PARALLELISM : resolveStageParallelism(root);
            int computeStageId = createStage(root, parallelism, "COMPUTE");
            setPlan(computeStageId, rewrite(root, computeStageId, parallelism));
            if (!needsRetrievalReducer && (parallelism == SINGLE_PARALLELISM
                    || terminalPurpose == StageTerminalPurpose.RELATIONAL_EXPORT)) {
                stages.get(computeStageId).taskType = "OUTPUT";
            } else {
                addOutputStage(computeStageId);
            }
            return build();
        }

        private static boolean containsCorrelatedExecution(RelNode node) {
            if (node instanceof LogicalCorrelate correlate) {
                RelNode right = correlate.getRight();
                boolean nativeUnnest = right instanceof Uncollect
                        || right instanceof LogicalProject project
                                && project.getInput() instanceof Uncollect;
                if (!correlate.getRequiredColumns().isEmpty() && !nativeUnnest) {
                    return true;
                }
                while (right instanceof LogicalProject project) {
                    right = project.getInput();
                }
                if (right instanceof LogicalAggregate aggregate
                        && aggregate.getGroupCount() == 0
                        && aggregate.getInput() instanceof LogicalFilter) {
                    return true;
                }
            }
            for (RelNode input : node.getInputs()) {
                if (containsCorrelatedExecution(input)) {
                    return true;
                }
            }
            return false;
        }

        /** Aggregates, windows and global ordering require a semantically complete input distribution. */
        private RelNode enforceGlobalOperators(RelNode node, boolean root) {
            if (liveRead) {
                node = projectBeforeSort(node);
            }
            if (!node.getInputs().isEmpty()) {
                List<RelNode> inputs = new ArrayList<>(node.getInputs().size());
                boolean changed = false;
                for (RelNode input : node.getInputs()) {
                    RelNode rewritten = enforceGlobalOperators(input, false);
                    inputs.add(rewritten);
                    changed |= rewritten != input;
                }
                if (changed) {
                    node = node.copy(node.getTraitSet(), inputs);
                }
            }
            if (liveRead && node instanceof LogicalJoin join) {
                node = materializeSelectiveBuild(join);
            }
            if (node instanceof Aggregate aggregate
                    && !hasOnlyLocalSourceLeaves(aggregate.getInput())) {
                // Repartitioning a singleton producer creates multiple independent
                // aggregate owners without adding scan parallelism. Gather its rows
                // at one owner so each group has exactly one final result.
                int producers = resolveStageParallelism(aggregate.getInput());
                if (aggregate instanceof LogicalAggregate logical) {
                    if (liveRead && containsDirectTableScan(logical.getInput())) {
                        var bindings = DecimalAggregateExchange.select(logical, this::functionCatalogues, producers,
                                driverEstimate.applyAsInt(producers));
                        if (!bindings.isEmpty()) {
                            return reduceDecimalBeforeExchange(logical, bindings);
                        }
                    }
                    if (canReduceBeforeExchange(logical, producers)) {
                        return reduceBeforeExchange(logical);
                    }
                }
                boolean parallelProducer = producers > SINGLE_PARALLELISM;
                PangrovaDistribution distribution = parallelProducer && (aggregate.getGroupSets() == null
                        || aggregate.getGroupSets().size() == 1)
                        && !aggregate.getGroupSet().isEmpty()
                        ? PangrovaDistribution.hash(aggregate.getGroupSet().asList())
                        : PangrovaDistribution.SINGLETON;
                RelNode input = enforceDistribution(aggregate.getInput(), distribution);
                if (input != aggregate.getInput()) {
                    return aggregate.copy(aggregate.getTraitSet(), input,
                            aggregate.getGroupSet(), aggregate.getGroupSets(),
                            aggregate.getAggCallList());
                }
            }
            if (node instanceof Window window && !hasOnlyLocalSourceLeaves(window.getInput())) {
                return WindowPartitioning.enforce(window, FragmentContext::enforceDistribution);
            }
            if (node instanceof Sort sort
                    && !(root && terminalPurpose == StageTerminalPurpose.QUERY_RESULT
                            && sort.fetch != null
                            && (sort.offset == null || RexLiteral.intValue(sort.offset) == 0))
                    && !hasOnlyLocalSourceLeaves(sort.getInput())) {
                RelNode input = enforceDistribution(sort.getInput(),
                        PangrovaDistribution.SINGLETON);
                if (input != sort.getInput()) {
                    return sort.copy(sort.getTraitSet(), input,
                            sort.getCollation(), sort.offset, sort.fetch);
                }
            }
            return node;
        }

        private static RelNode projectBeforeSort(RelNode node) {
            // Calcite can place a narrow projection above Sort. Move its field
            // selection below the sort before enforcing the gather distribution.
            if (!(node instanceof LogicalProject project) || !(project.getInput() instanceof Sort sort)
                    || project.getProjects().isEmpty()
                    || !project.getProjects().stream().allMatch(RexInputRef.class::isInstance)) {
                return node;
            }
            var input = sort.getInput();
            if (!(input instanceof LogicalTableScan)
                    && !(input instanceof LogicalFilter filter && filter.getInput() instanceof LogicalTableScan)) {
                return node;
            }
            var fields = project.getProjects().stream().map(RexInputRef.class::cast)
                    .map(RexInputRef::getIndex).toList();
            if (fields.size() >= input.getRowType().getFieldCount()
                    || new java.util.HashSet<>(fields).size() != fields.size()) {
                return node;
            }
            var ordering = sort.getCollation().getFieldCollations();
            if (ordering.stream().anyMatch(key -> !fields.contains(key.getFieldIndex()))) {
                return node;
            }
            var remapped = RelCollations.of(ordering.stream()
                    .map(key -> key.withFieldIndex(fields.indexOf(key.getFieldIndex()))).toList());
            var projection = project.copy(input.getTraitSet().replace(RelCollations.EMPTY),
                    input, project.getProjects(), project.getRowType());
            return sort.copy(sort.getTraitSet().replace(remapped), projection, remapped, sort.offset, sort.fetch);
        }

        private static boolean canReduceBeforeExchange(LogicalAggregate aggregate, int producers) {
            int groupCount = aggregate.getGroupCount();
            if (producers <= SINGLE_PARALLELISM
                    || aggregate.getGroupSets().size() != 1 || aggregate.getAggCallList().isEmpty()
                    || !containsDirectTableScan(aggregate.getInput())
                    || !aggregate.getAggCallList().stream().allMatch(FragmentContext::hasExactScalarMerge)) {
                return false;
            }
            var metadata = aggregate.getCluster().getMetadataQuery();
            var input = aggregate.getInput();
            Double inputRows = metadata.getRowCount(input);
            if (inputRows == null || !Double.isFinite(inputRows) || inputRows <= producers) return false;
            if (groupCount == 0) return true;
            // Charge for each observed combination when joint cardinality is unavailable.
            double groups = 1;
            for (int ordinal : aggregate.getGroupSet()) {
                var origin = metadata.getColumnOrigin(input, ordinal);
                if (origin == null || origin.isDerived()) {
                    LOG.debug("event=group_aggregate_distribution_retained reason=missing_direct_column_origin");
                    return false;
                }
                var source = origin.getOriginTable();
                var table = source.unwrap(MetadataResolvedTable.class);
                if (table == null) {
                    LOG.debug("event=group_aggregate_distribution_retained reason=no_selected_connector_snapshot");
                    return false;
                }
                var column = source.getRowType().getFieldNames().get(origin.getOriginColumnOrdinal());
                var statistics = table.selectedColumnStatistics(column);
                if (statistics.ndv() < 0) {
                    LOG.debug("event=group_aggregate_distribution_retained reason=unknown_column_cardinality column={}",
                            column);
                    return false;
                }
                double cardinality = statistics.ndv() + (statistics.nullCount() == 0 ? 0.0 : 1.0);
                groups *= Math.max(cardinality, 1);
                if (groups * producers >= inputRows) return false;
            }
            double partialRows = groups * producers;
            var rawWidth = metadata.getAverageRowSize(input);
            var resultWidth = metadata.getAverageRowSize(aggregate);
            LOG.debug("event=group_aggregate_exchange_estimates input_rows={} partial_rows={}"
                    + " input_row_bytes={} partial_row_bytes={} producers={}",
                    inputRows, partialRows, rawWidth, resultWidth, producers);
            return partialRows < inputRows && rawWidth != null && resultWidth != null
                    && Double.isFinite(rawWidth) && Double.isFinite(resultWidth) && rawWidth > 0 && resultWidth > 0
                    && partialRows * resultWidth < inputRows * rawWidth;
        }

        private static boolean hasExactScalarMerge(AggregateCall call) {
            if (call.isDistinct() || call.isApproximate() || call.ignoreNulls() || call.distinctKeys != null
                    || call.hasCollation() || !call.rexList.isEmpty()) {
                return false;
            }
            return switch (call.getAggregation().getKind()) {
                case COUNT -> true;
                case MIN, MAX -> switch (call.getType().getSqlTypeName()) {
                    case TINYINT, SMALLINT, INTEGER, BIGINT -> true;
                    default -> false;
                };
                default -> false;
            };
        }

        private List<FunctionCatalogView> functionCatalogues() {
            if (functionCatalogues == null) {
                functionCatalogues = catalogueSource.get();
            }
            return functionCatalogues;
        }

        private static RelNode reduceDecimalBeforeExchange(LogicalAggregate aggregate,
                List<AggregateExchangeBinding> bindings) {
            var input = aggregate.getInput();
            var typeFactory = aggregate.getCluster().getTypeFactory();
            var stateType = typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.VARBINARY), true);
            var fields = typeFactory.builder();
            for (var field : aggregate.getRowType().getFieldList()) {
                fields.add(field.getName(), stateType);
            }
            var calls = aggregate.getAggCallList();
            var local = new StageAggregate(input.getTraitSet().replace(PangrovaDistribution.ANY), input,
                    calls, fields.build(), AggregationPhase.AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE, bindings);
            var exchanged = enforceDistribution(local, PangrovaDistribution.SINGLETON);
            return new StageAggregate(aggregate.getTraitSet(), exchanged, calls, aggregate.getRowType(),
                    AggregationPhase.AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT, bindings);
        }

        private static RelNode reduceBeforeExchange(LogicalAggregate aggregate) {
            RelNode input = aggregate.getInput();
            var local = aggregate.copy(input.getTraitSet().replace(PangrovaDistribution.ANY), input,
                    aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
            int groupCount = aggregate.getGroupCount();
            var groupKeys = ImmutableBitSet.range(groupCount);
            var distribution = groupCount == 0 ? PangrovaDistribution.SINGLETON
                    : PangrovaDistribution.hash(groupKeys.asList());
            RelNode exchanged = enforceDistribution(local, distribution);
            List<AggregateCall> merges = new ArrayList<>(aggregate.getAggCallList().size());
            for (int index = 0; index < aggregate.getAggCallList().size(); index++) {
                AggregateCall call = aggregate.getAggCallList().get(index);
                var operator = call.getAggregation().getKind() == SqlKind.COUNT
                        ? SqlStdOperatorTable.SUM0 : call.getAggregation();
                // Only the local measure owns the raw argument and FILTER mask.
                // SUM0 preserves COUNT's zero for no input. MIN and MAX retain null.
                merges.add(AggregateCall.create(call.getParserPosition(), operator,
                        false, false, false, List.of(), List.of(groupCount + index), -1, null, RelCollations.EMPTY,
                        call.getType(), call.getName()));
            }
            return aggregate.copy(aggregate.getTraitSet(), exchanged, groupKeys, List.of(groupKeys), merges);
        }

        private static RelNode enforceDistribution(
                RelNode input, PangrovaDistribution distribution) {
            ExchangeBoundary boundary = exchangeBoundary(input);
            if (boundary != null && boundary.distribution().equals(distribution)) {
                return input;
            }
            return new PangrovaExchange(input.getCluster(),
                    input.getTraitSet().replace(distribution), input, distribution);
        }

        private RelNode rewrite(RelNode node, int stageId, int stageParallelism) {
            ExchangeBoundary boundary = exchangeBoundary(node);
            if (boundary != null) {
                return materializeBoundary(boundary, stageId, stageParallelism);
            }
            if (node.getInputs().isEmpty()) {
                return node;
            }
            List<RelNode> rewrittenInputs = new ArrayList<>(node.getInputs().size());
            boolean changed = false;
            for (RelNode input : node.getInputs()) {
                RelNode rewritten = rewrite(input, stageId, stageParallelism);
                rewrittenInputs.add(rewritten);
                changed |= rewritten != input;
            }
            RelNode rewritten = changed ? node.copy(node.getTraitSet(), rewrittenInputs) : node;
            if (liveRead && rewritten instanceof Join join && (join instanceof PangrovaHashJoin
                    || join instanceof PangrovaBroadcastJoin) && runtimeFilters.size() < 2) {
                eligibleRuntimeFilter(join).ifPresent(runtimeFilters::add);
            }
            return rewritten;
        }

        private RelNode materializeBoundary(
                ExchangeBoundary boundary,
                int downstreamStageId,
                int downstreamParallelism) {
            int producerParallelism = resolveStageParallelism(boundary.input());
            int upstreamStageId = createStage(
                    boundary.input(), producerParallelism, "COMPUTE");
            RelNode producerInput = rewrite(
                    boundary.input(), upstreamStageId, producerParallelism);
            PangrovaExchange producer = boundProducer(
                    boundary, producerInput, downstreamParallelism);
            setPlan(upstreamStageId, producer);

            String exchangeId = exchangeId(upstreamStageId, downstreamStageId);
            addTransfer(boundary.distribution(), exchangeId,
                    upstreamStageId, downstreamStageId, downstreamParallelism);
            return new PangrovaExchangeInput(
                    boundary.node().getCluster(),
                    boundary.node().getTraitSet(),
                    boundary.node().getRowType(),
                    new PangrovaExchangeInput.Specification(
                            "exchange-input-" + exchangeId,
                            exchangeId,
                            upstreamStageId,
                            downstreamStageId,
                            boundary.distribution(),
                            downstreamParallelism,
                            EXCHANGE_SERDE,
                            true),
                    new PlanFragment(producerInput, 0, 0).resultColumnModels());
        }

        private PangrovaExchange boundProducer(
                ExchangeBoundary boundary,
                RelNode producerInput,
                int partitionCount) {
            PangrovaDistribution distribution = boundary.distribution();
            validateMaterializedDistribution(distribution);
            return new PangrovaExchange(
                    boundary.node().getCluster(),
                    boundary.node().getTraitSet(),
                    producerInput,
                    distribution,
                    partitionCount);
        }

        private void addOutputStage(int computeStageId) {
            MutableStage compute = stages.get(computeStageId);
            RelNode computeRoot = compute.plan;
            int outputStageId = createStage(
                    computeRoot, SINGLE_PARALLELISM, "OUTPUT");
            String exchangeId = exchangeId(computeStageId, outputStageId);
            PangrovaExchange producer = new PangrovaExchange(
                    computeRoot.getCluster(),
                    computeRoot.getTraitSet(),
                    computeRoot,
                    PangrovaDistribution.SINGLETON,
                    SINGLE_PARALLELISM);
            compute.plan = producer;
            addTransfer(PangrovaDistribution.SINGLETON, exchangeId,
                    computeStageId, outputStageId, SINGLE_PARALLELISM);
            RelNode exchangeInput = new PangrovaExchangeInput(
                    compute.plan.getCluster(),
                    compute.plan.getTraitSet(),
                    compute.plan.getRowType(),
                    new PangrovaExchangeInput.Specification(
                            "exchange-input-" + exchangeId,
                            exchangeId,
                            computeStageId,
                            outputStageId,
                            PangrovaDistribution.SINGLETON,
                            SINGLE_PARALLELISM,
                            EXCHANGE_SERDE,
                            true),
                    new PlanFragment(computeRoot, 0, 0).resultColumnModels());
            setPlan(outputStageId, finalReducer(computeRoot, exchangeInput));
        }

        /** Native reapplies Top-K after the singleton boundary to globally order task-local candidates. */
        private static RelNode finalReducer(RelNode computeRoot, RelNode exchangeInput) {
            if (computeRoot instanceof Sort sort
                    && sort.fetch != null
                    && (sort.offset == null || RexLiteral.intValue(sort.offset) == 0)) {
                return sort.copy(
                        sort.getTraitSet(),
                        exchangeInput,
                        sort.getCollation(),
                        sort.offset,
                        sort.fetch);
            }
            return exchangeInput;
        }

        private void addTransfer(
                PangrovaDistribution distribution,
                String exchangeId,
                int upstreamStageId,
                int downstreamStageId,
                int partitionCount) {
            dependencies.computeIfAbsent(
                    downstreamStageId, ignored -> new ArrayList<>()).add(upstreamStageId);
            transfers.add(new StageTransfer(
                    upstreamStageId,
                    downstreamStageId,
                    toShuffleType(distribution),
                    distribution.keys(),
                    exchangeId,
                    partitionCount,
                    EXCHANGE_SERDE,
                    true));
        }

        private int resolveStageParallelism(RelNode root) {
            List<ExchangeBoundary> boundaries = new ArrayList<>();
            collectDirectBoundaries(root, boundaries);
            // A splitless local source is replicated verbatim by every task.
            // Keep VALUES and Native GENERATE_SERIES on exactly one producer;
            // an override must not multiply rows or provider calls.
            if (containsDirectLocalSourceLeaf(root)) {
                return SINGLE_PARALLELISM;
            }
            // A join can run in parallel only when each input has a proven
            // compatible exchange contract. Merely having an exchange elsewhere
            // in the stage does not co-locate matching rows.
            if (containsJoin(root) && (boundaries.isEmpty()
                    || containsDirectTableScan(root)
                    || !hasCompatibleJoinExchanges(root))) {
                return SINGLE_PARALLELISM;
            }
            if (boundaries.stream().anyMatch(FragmentContext::isSingleton)) {
                return SINGLE_PARALLELISM;
            }
            if (sessionParallelismOverride > 0) {
                return Math.min(sessionParallelismOverride, maxParallelism);
            }
            if (boundaries.isEmpty()) {
                return computeDynamicParallelism(root);
            }
            return boundaries.stream()
                    .filter(boundary -> boundary.distribution().type()
                            != DistributionType.BROADCAST)
                    .mapToInt(boundary -> computeDynamicParallelism(boundary.input()))
                    .max()
                    .orElseGet(() -> computeDynamicParallelism(root));
        }

        private static boolean hasOnlyLocalSourceLeaves(RelNode node) {
            if (exchangeBoundary(node) != null) {
                return false;
            }
            if (node.getInputs().isEmpty()) {
                return node instanceof Values
                        || node instanceof LogicalTableFunctionScan scan
                                && scan.getCall() instanceof RexCall call
                                && call.getOperator().getName().equalsIgnoreCase("GENERATE_SERIES")
                                && scan.getInputs().isEmpty();
            }
            return node.getInputs().stream()
                    .allMatch(FragmentContext::hasOnlyLocalSourceLeaves);
        }

        /** A local source in a mixed stage also runs once, while exchange inputs are separate stages. */
        private static boolean containsDirectLocalSourceLeaf(RelNode node) {
            if (exchangeBoundary(node) != null) {
                return false;
            }
            if (node.getInputs().isEmpty()) {
                return hasOnlyLocalSourceLeaves(node);
            }
            return node.getInputs().stream()
                    .anyMatch(FragmentContext::containsDirectLocalSourceLeaf);
        }

        private static boolean containsJoin(RelNode node) {
            if (exchangeBoundary(node) != null) {
                return false;
            }
            if (node instanceof Join) {
                return true;
            }
            return node.getInputs().stream().anyMatch(FragmentContext::containsJoin);
        }

        private static boolean hasCompatibleJoinExchanges(RelNode node) {
            if (exchangeBoundary(node) != null) {
                return true;
            }
            if (node instanceof Join join) {
                ExchangeBoundary left = exchangeBoundary(join.getLeft());
                ExchangeBoundary right = exchangeBoundary(join.getRight());
                if (left == null || right == null) {
                    return false;
                }
                PangrovaDistribution leftDist = left.distribution();
                PangrovaDistribution rightDist = right.distribution();
                if (rightDist.type() == DistributionType.BROADCAST) {
                    // Unmatched build-side rows would be emitted on every task.
                    JoinRelType joinType = join.getJoinType();
                    return leftDist.type() != DistributionType.BROADCAST
                            && (joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT
                                    || joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI);
                }
                var keys = join.analyzeCondition();
                return leftDist.type() == DistributionType.HASH
                        && rightDist.type() == DistributionType.HASH
                        && !keys.leftKeys.isEmpty()
                        && leftDist.keys().equals(keys.leftKeys)
                        && rightDist.keys().equals(keys.rightKeys);
            }
            return node.getInputs().stream().allMatch(FragmentContext::hasCompatibleJoinExchanges);
        }

        /** A scan without an exchange is not partitioned by a downstream join key. */
        private static boolean containsDirectTableScan(RelNode node) {
            if (exchangeBoundary(node) != null) {
                return false;
            }
            if (node instanceof TableScan) {
                return true;
            }
            return node.getInputs().stream().anyMatch(FragmentContext::containsDirectTableScan);
        }

        private static boolean isSingleton(ExchangeBoundary boundary) {
            return boundary.distribution().type() == DistributionType.SINGLETON;
        }

        private void collectDirectBoundaries(
                RelNode node,
                List<ExchangeBoundary> boundaries) {
            ExchangeBoundary boundary = exchangeBoundary(node);
            if (boundary != null) {
                boundaries.add(boundary);
                return;
            }
            for (RelNode input : node.getInputs()) {
                collectDirectBoundaries(input, boundaries);
            }
        }

        private int computeDynamicParallelism(RelNode node) {
            return FragmentSourceParallelism.resolve(node, bytesPerPartition,
                    maxParallelism, DEFAULT_PARALLELISM);
        }

        private int createStage(RelNode plan, int parallelism, String taskType) {
            int stageId = nextStageId++;
            stages.put(stageId, new MutableStage(
                    stageId, plan, parallelism, taskType));
            return stageId;
        }

        private void setPlan(int stageId, RelNode plan) {
            MutableStage stage = stages.get(stageId);
            if (stage == null) {
                throw new IllegalStateException("Stage not found: " + stageId);
            }
            stage.plan = plan;
        }

        private FragmentedPlan build() {
            List<PlanStage> planStages = stages.values().stream()
                    .sorted(Comparator.comparingInt(stage -> stage.id))
                    .map(FragmentContext::immutableStage)
                    .toList();
            Map<Integer, List<Integer>> immutableDependencies = new LinkedHashMap<>();
            dependencies.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> immutableDependencies.put(
                            entry.getKey(), List.copyOf(entry.getValue())));
            Optional<StageRuntimeFilter> filter = runtimeFilters.size() == 1
                    ? Optional.of(runtimeFilters.get(0)).filter(candidate ->
                            countPhysicalScans(stages.get(candidate.probeStageId()).plan) == 1)
                    : Optional.empty();
            return new FragmentedPlan(planStages, transfers, immutableDependencies,
                    filter);
        }

        private Optional<StageRuntimeFilter> eligibleRuntimeFilter(Join join) {
            if (!(join.getRight() instanceof PangrovaExchangeInput build)) {
                return Optional.empty();
            }
            RelNode probeInput = join.getLeft();
            int buildStage = build.upstreamStageId();
            if (dependencies.containsKey(buildStage)) return Optional.empty();
            int probeStage = probeInput instanceof PangrovaExchangeInput exchange
                    ? exchange.upstreamStageId() : build.downstreamStageId();
            if (probeInput instanceof PangrovaExchangeInput) {
                if (dependencies.containsKey(probeStage)
                        || !(stages.get(probeStage).plan instanceof PangrovaExchange producer)) {
                    return Optional.empty();
                }
                probeInput = producer.getInput();
            } else if (!List.of(buildStage).equals(dependencies.get(build.downstreamStageId()))) {
                return Optional.empty();
            }
            return eligibleRuntimeFilterKey(join, probeInput).map(key -> new StageRuntimeFilter(
                    buildStage, build.exchangeId(), key.buildOrdinal(), probeStage,
                    key.probe().read(), key.probe().fieldId(), key.probe().name()));
        }

        private static RelNode materializeSelectiveBuild(LogicalJoin join) {
            var probe = eligibleRuntimeFilterKey(join, join.getLeft()).map(RuntimeFilterKey::probe).orElse(null);
            if (probe == null || !StageRuntimeFilter.enoughProbeWork(probe.read().statistics())) return join;

            var build = directBuildRead(join.getRight()).orElse(null);
            if (build == null || build.sourceIdentity().equals(probe.read().sourceIdentity())) return join;
            var size = build.statistics();
            long maxBytes = 8L * 1024 * 1024;
            if (size.rowCount() < 0 || size.rowCount() > StageRuntimeFilter.MAX_BUILD_ROWS
                    || size.dataSize() < 0 || size.dataSize() > maxBytes
                    || size.rowCount() > probe.read().statistics().rowCount() / 4) {
                return join;
            }
            RelNode exchange = enforceDistribution(join.getRight(), PangrovaDistribution.BROADCAST);
            return new PangrovaBroadcastJoin(new BroadcastJoinConfig(join.getCluster(), join.getTraitSet(),
                    join.getLeft(), exchange, join.getCondition(), join.getVariablesSet(), join.getJoinType()));
        }

        private static Optional<org.pangrova.connector.api.metadata.table.ConnectorReadBinding> directBuildRead(
                RelNode node) {
            if (node instanceof Project project
                    && project.getProjects().stream().allMatch(RexInputRef.class::isInstance)) {
                return directBuildRead(project.getInput());
            }
            if (node instanceof Filter filter) return directBuildRead(filter.getInput());
            return node instanceof TableScan scan ? selectedRead(scan) : Optional.empty();
        }

        private static Optional<RuntimeFilterKey> eligibleRuntimeFilterKey(Join join, RelNode probeInput) {
            if (join.getJoinType() != JoinRelType.INNER || !join.getVariablesSet().isEmpty()) {
                return Optional.empty();
            }
            int leftCount = join.getLeft().getRowType().getFieldCount();
            for (var conjunct : RelOptUtil.conjunctions(join.getCondition())) {
                if (!(conjunct instanceof RexCall equality)
                        || equality.getKind() != SqlKind.EQUALS
                        || !(equality.operands.get(0) instanceof RexInputRef first)
                        || !(equality.operands.get(1) instanceof RexInputRef second)
                        || first.getType().getSqlTypeName() != SqlTypeName.BIGINT
                        || second.getType().getSqlTypeName() != SqlTypeName.BIGINT) {
                    continue;
                }
                int probeOrdinal = Math.min(first.getIndex(), second.getIndex());
                int buildOrdinal = Math.max(first.getIndex(), second.getIndex()) - leftCount;
                if (probeOrdinal < leftCount && buildOrdinal >= 0) {
                    var probe = directProbeColumn(probeInput, probeOrdinal);
                    if (probe.isPresent()) {
                        return Optional.of(new RuntimeFilterKey(buildOrdinal, probe.orElseThrow()));
                    }
                }
            }
            return Optional.empty();
        }

        private record RuntimeFilterKey(int buildOrdinal, ProbeColumn probe) {}

        private static int countPhysicalScans(RelNode node) {
            if (node instanceof TableScan) return 1;
            int count = 0;
            for (RelNode input : node.getInputs()) {
                count += countPhysicalScans(input);
                if (count > 1) break;
            }
            return count;
        }

        private record ProbeColumn(org.pangrova.connector.api.metadata.table.ConnectorReadBinding read,
                                   int fieldId, String name) {}

        private static Optional<ProbeColumn> directProbeColumn(RelNode node, int ordinal) {
            if (node instanceof Project project && project.getProjects().get(ordinal) instanceof RexInputRef ref) {
                return directProbeColumn(project.getInput(), ref.getIndex());
            }
            if (node instanceof Filter filter) return directProbeColumn(filter.getInput(), ordinal);
            if (!(node instanceof TableScan scan)
                    || scan.getRowType().getFieldList().get(ordinal).getType().getSqlTypeName()
                            != SqlTypeName.BIGINT) {
                return Optional.empty();
            }
            String name = scan.getRowType().getFieldNames().get(ordinal);
            return selectedRead(scan)
                    .filter(read -> read.snapshotId().isPresent() && read.handle().incrementalRange().isEmpty())
                    .flatMap(read -> read.schemaIdentity()
                            .filter(schema -> "ICEBERG".equals(schema.tableIdentity().format()))
                            .stream().flatMap(schema -> schema.fields().stream())
                            .filter(field -> field.name().equals(name))
                            .findFirst().map(field -> new ProbeColumn(read, field.fieldId().orElseThrow(), name)));
        }

        private static Optional<org.pangrova.connector.api.metadata.table.ConnectorReadBinding> selectedRead(
                TableScan scan) {
            var table = PlanTable.require(scan).unwrap(MetadataResolvedTable.class);
            return table == null ? Optional.empty() : table.selectedReadBinding();
        }

        private static PlanStage immutableStage(MutableStage stage) {
            PlanFragment fragment = new PlanFragment(
                    stage.plan,
                    PlanFragment.estimateBytes(stage.plan),
                    PlanFragment.estimateRows(stage.plan));
            return new PlanStage(
                    stage.id, fragment, stage.parallelism, stage.taskType);
        }

        private static ExchangeBoundary exchangeBoundary(RelNode node) {
            if (node instanceof PangrovaExchange exchange) {
                return new ExchangeBoundary(
                        node, exchange.getInput(), exchange.targetDistribution());
            }
            if (node instanceof Exchange exchange) {
                return new ExchangeBoundary(
                        node, exchange.getInput(), mapDistribution(exchange.getDistribution()));
            }
            return null;
        }

        private static PangrovaDistribution mapDistribution(RelDistribution distribution) {
            return switch (distribution.getType()) {
                case HASH_DISTRIBUTED -> PangrovaDistribution.hash(
                        distribution.getKeys().stream().map(Integer::valueOf).toList());
                case BROADCAST_DISTRIBUTED -> PangrovaDistribution.BROADCAST;
                case RANDOM_DISTRIBUTED, ROUND_ROBIN_DISTRIBUTED ->
                        PangrovaDistribution.ROUND_ROBIN;
                case SINGLETON -> PangrovaDistribution.SINGLETON;
                case RANGE_DISTRIBUTED -> throw new IllegalArgumentException(
                        "Range exchange requires typed partition bounds");
                case ANY -> throw new IllegalArgumentException(
                        "ANY distribution cannot materialize an exchange");
            };
        }

        private static void validateMaterializedDistribution(
                PangrovaDistribution distribution) {
            if (distribution.type() == DistributionType.ANY) {
                throw new IllegalArgumentException(
                        "ANY distribution cannot materialize an exchange");
            }
            if (distribution.type() == DistributionType.HASH
                    && distribution.keys().isEmpty()) {
                throw new IllegalArgumentException(
                        "Hash exchange requires at least one partition key");
            }
        }

        private static String exchangeId(int upstreamStageId, int downstreamStageId) {
            return "exchange-" + upstreamStageId + "-to-" + downstreamStageId;
        }
    }

    private record ExchangeBoundary(
            RelNode node,
            RelNode input,
            PangrovaDistribution distribution) {}

    private static final class MutableStage {
        private final int id;
        private final int parallelism;
        private RelNode plan;
        private String taskType;

        private MutableStage(
                int id,
                RelNode plan,
                int parallelism,
                String taskType) {
            this.id = id;
            this.plan = plan;
            this.parallelism = parallelism;
            this.taskType = taskType;
        }
    }
}
