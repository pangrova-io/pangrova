package org.pangrova.coordinator.feature;

import org.pangrova.generation.api.enums.GenerationExchangeDistribution;

import org.pangrova.planner.enums.StageTerminalPurpose;
import org.pangrova.generation.api.enums.GenerationGraphPurpose;
import org.pangrova.generation.execution.template.FrozenStageBindings;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.StageSplitPartitions;
import org.pangrova.generation.api.page.GenerationPage;
import org.pangrova.generation.api.page.GenerationSplit;
import org.pangrova.generation.api.graph.GenerationStageGraph;
import org.pangrova.generation.api.enums.GenerationPageKind;
import org.pangrova.planner.analysis.AuthorizedStageGraph;
import org.pangrova.planner.generation.TabularExportPlanBuilder;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.OperatorBinding;

import java.util.ArrayList;
import java.util.List;

/** Persists the authorized ordinary graph and exact source allocations, adding only its terminal export sink. */
final class FeatureExportStageGraph {
    private FeatureExportStageGraph() {}

    record Prepared(GenerationStageGraph graph, List<GenerationPage> pages) {}

    static Prepared prepare(AuthorizedStageGraph graph, String jobId, String sourceId,
            List<GenerationSplit> splits, OperatorBinding sink, int maxAttempts) {
        requireAdmitted(graph, sourceId);
        if (splits.stream().map(GenerationSplit::canonicalSplitBase64).distinct().count() != splits.size()) {
            throw new IllegalArgumentException("Feature export repeats a canonical source split");
        }
        var stages = new ArrayList<GenerationStageGraph.StagePlan>();
        var pages = new ArrayList<GenerationPage>();
        var consumers = graph.transfers().stream().map(transfer -> transfer.downstreamStageId())
                .collect(java.util.stream.Collectors.toSet());
        for (var stage : topologicalStages(graph)) {
            boolean terminal = "OUTPUT".equals(stage.taskType());
            var plan = terminal ? TabularExportPlanBuilder.build(stage.serializedPlan().planBytes()).planBytes()
                    : stage.serializedPlan().planBytes();
            try {
                var root = org.pangrova.protocol.substrait.SubstraitPlanCodec
                        .parse(plan).getRelations(0).getRoot();
                SubstraitRelationLayout.rootSchema(root);
            } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                throw new IllegalArgumentException("Feature export stage " + stage.stageId()
                        + " has an invalid Substrait plan", failure);
            } catch (IllegalArgumentException failure) {
                throw new IllegalArgumentException("Feature export stage " + stage.stageId()
                        + " has an invalid canonical schema: " + failure.getMessage(), failure);
            }
            var bound = new BoundExecutionPlan(plan, terminal ? List.of(sink) : List.of());
            var kind = terminal ? GenerationPageKind.RELATIONAL_PARTITION : GenerationPageKind.RELATIONAL_COMPUTE;
            stages.add(GenerationStageGraph.StagePlan.of(stage.stageId(), stage.parallelism(), kind, plan,
                    FrozenStageBindings.freeze(bound, List.of()),
                    stage.sources().stream().map(source -> source.qualifiedName()).toList(),
                    stage.outputNames(), terminal));
            pages.addAll(pages(stage, jobId, consumers.contains(stage.stageId()) ? List.of() : splits,
                    kind, maxAttempts, pages.size()));
        }
        var edges = graph.transfers().stream().map(edge -> new GenerationStageGraph.Exchange(edge.exchangeId(),
                edge.upstreamStageId(), edge.downstreamStageId(),
                GenerationExchangeDistribution.valueOf(edge.shuffleType().name()), edge.distributionKeys(),
                edge.partitionCount(), edge.batchSerde().name(), edge.requireComplete())).toList();
        return new Prepared(new GenerationStageGraph(GenerationStageGraph.FORMAT_VERSION,
                GenerationGraphPurpose.RELATIONAL_EXPORT, stages, edges, List.of()), List.copyOf(pages));
    }

    private static List<AuthorizedStageGraph.SerializedStage> topologicalStages(AuthorizedStageGraph graph) {
        var stages = graph.stages().stream().collect(java.util.stream.Collectors.toMap(
                AuthorizedStageGraph.SerializedStage::stageId, java.util.function.Function.identity()));
        var inDegree = new java.util.HashMap<Integer, Integer>();
        var downstream = new java.util.HashMap<Integer, List<Integer>>();
        stages.keySet().forEach(id -> inDegree.put(id, 0));
        for (var transfer : graph.transfers()) {
            inDegree.compute(transfer.downstreamStageId(), (id, count) -> count + 1);
            downstream.computeIfAbsent(transfer.upstreamStageId(), id -> new ArrayList<>())
                    .add(transfer.downstreamStageId());
        }
        var ready = new java.util.TreeSet<Integer>();
        inDegree.forEach((id, count) -> { if (count == 0) ready.add(id); });
        var ordered = new ArrayList<AuthorizedStageGraph.SerializedStage>();
        while (!ready.isEmpty()) {
            int id = ready.pollFirst();
            ordered.add(stages.get(id));
            for (int target : downstream.getOrDefault(id, List.of())) {
                if (inDegree.compute(target, (key, count) -> count - 1) == 0) ready.add(target);
            }
        }
        if (ordered.size() != stages.size()) throw new IllegalArgumentException("Feature stage graph contains a cycle");
        return List.copyOf(ordered);
    }

    private static List<GenerationPage> pages(AuthorizedStageGraph.SerializedStage stage,
            String job, List<GenerationSplit> splits, GenerationPageKind kind, int maxAttempts, int ordinalBase) {
        var allocations = splits.isEmpty()
                ? java.util.Collections.nCopies(stage.parallelism(), List.<GenerationSplit>of())
                : StageSplitPartitions.partitionSplits(splits, stage.parallelism(), ignored -> 1L);
        var result = new ArrayList<GenerationPage>(stage.parallelism());
        for (int partition = 0; partition < stage.parallelism(); partition++) {
            result.add(new GenerationPage(job, pageId(job, stage.stageId(), partition),
                    stage.stageId(), partition, Math.addExact(ordinalBase, partition), kind,
                    allocations.get(partition), List.of(), maxAttempts));
        }
        return List.copyOf(result);
    }

    private static String pageId(String job, int stage, int partition) {
        return job + ":stage:" + stage + ":partition:" + partition;
    }

    static void requireAdmitted(AuthorizedStageGraph graph, String sourceId) {
        if (graph.terminalPurpose() != StageTerminalPurpose.RELATIONAL_EXPORT) {
            throw new IllegalArgumentException(
                    "Feature materialization requires the protected export-purpose stage graph");
        }
        for (var stage : graph.stages()) {
            if (!stage.serializedPlan().aiInvocationHints().isEmpty()
                    || !stage.serializedPlan().retrievalTopKHints().isEmpty()
                    || !stage.resultColumnModels().isEmpty()) {
                throw new IllegalArgumentException(
                        "Feature model/retrieval stages require actual frozen current model bindings");
            }
            if (stage.sources().stream().anyMatch(source -> !source.qualifiedName().equals(sourceId))) {
                throw new IllegalArgumentException(
                        "Feature stage source differs from its exact authenticated snapshot");
            }
        }
    }
}
