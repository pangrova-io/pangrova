package org.pangrova.coordinator.feature;

import org.pangrova.coordinator.artifact.template.FrozenStageBindings;
import org.pangrova.coordinator.query.execution.dispatch.dag.binding.StageSplitPartitions;
import org.pangrova.indexing.control.ArtifactBuildPage;
import org.pangrova.indexing.control.ArtifactBuildSplit;
import org.pangrova.indexing.control.ArtifactStageGraph;
import org.pangrova.indexing.api.ArtifactBuildPageKind;
import org.pangrova.planner.AuthorizedStageGraph;
import org.pangrova.planner.artifact.TabularExportPlanBuilder;
import org.pangrova.protocol.BoundExecutionPlan;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.OperatorBinding;

import java.util.ArrayList;
import java.util.List;

/** Persists the authorized ordinary graph and exact source allocations, adding only its terminal export sink. */
final class FeatureExportStageGraph {
    private FeatureExportStageGraph() {}

    record Prepared(ArtifactStageGraph graph, List<ArtifactBuildPage> pages) {}

    static Prepared prepare(AuthorizedStageGraph graph, String jobId, String sourceId,
            List<ArtifactBuildSplit> splits, OperatorBinding sink, int maxAttempts) {
        requireAdmitted(graph, sourceId);
        var stages = new ArrayList<ArtifactStageGraph.StagePlan>();
        var pages = new ArrayList<ArtifactBuildPage>();
        for (var stage : topologicalStages(graph)) {
            boolean terminal = "OUTPUT".equals(stage.taskType());
            var plan = terminal ? TabularExportPlanBuilder.build(stage.serializedPlan().planBytes()).planBytes()
                    : stage.serializedPlan().planBytes();
            try {
                var root = io.substrait.proto.Plan.parseFrom(plan).getRelations(0).getRoot();
                SubstraitRelationLayout.rootSchema(root);
            } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                throw new IllegalArgumentException("Feature export stage " + stage.stageId()
                        + " has an invalid Substrait plan", failure);
            } catch (IllegalArgumentException failure) {
                throw new IllegalArgumentException("Feature export stage " + stage.stageId()
                        + " has an invalid canonical schema: " + failure.getMessage(), failure);
            }
            var bound = new BoundExecutionPlan(plan, terminal ? List.of(sink) : List.of());
            var kind = terminal ? ArtifactBuildPageKind.RELATIONAL_PARTITION : ArtifactBuildPageKind.RELATIONAL_COMPUTE;
            stages.add(ArtifactStageGraph.StagePlan.of(stage.stageId(), stage.parallelism(), kind, plan,
                    FrozenStageBindings.freeze(bound, List.of()),
                    stage.sources().stream().map(source -> source.qualifiedName()).toList(),
                    stage.outputNames(), terminal));
            pages.addAll(pages(graph, stage, jobId, splits, kind, maxAttempts, pages.size()));
        }
        var edges = graph.transfers().stream().map(edge -> new ArtifactStageGraph.Exchange(edge.exchangeId(),
                edge.upstreamStageId(), edge.downstreamStageId(),
                ArtifactStageGraph.Exchange.Distribution.valueOf(edge.shuffleType().name()), edge.distributionKeys(),
                edge.partitionCount(), edge.batchSerde().name(), edge.requireComplete())).toList();
        return new Prepared(new ArtifactStageGraph(ArtifactStageGraph.FORMAT_VERSION,
                ArtifactStageGraph.Purpose.RELATIONAL_EXPORT, stages, edges, List.of()), List.copyOf(pages));
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

    private static List<ArtifactBuildPage> pages(AuthorizedStageGraph graph, AuthorizedStageGraph.SerializedStage stage,
            String job, List<ArtifactBuildSplit> splits, ArtifactBuildPageKind kind, int maxAttempts, int ordinalBase) {
        var producers = graph.transfers().stream().filter(edge -> edge.downstreamStageId() == stage.stageId())
                .map(edge -> graph.stages().stream().filter(value -> value.stageId() == edge.upstreamStageId())
                        .findFirst().orElseThrow()).toList();
        var dependencies = producers.stream().flatMap(producer -> java.util.stream.IntStream
                .range(0, producer.parallelism()).mapToObj(partition -> pageId(job, producer.stageId(), partition)))
                .toList();
        var byBytes = splits.stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                ArtifactBuildSplit::canonicalSplitBase64, java.util.function.Function.identity()));
        var allocations = producers.isEmpty() ? StageSplitPartitions.partitionSplits(
                splits.stream().map(ArtifactBuildSplit::canonicalSplitBase64).toList(), stage.parallelism())
                : List.<List<String>>of();
        var result = new ArrayList<ArtifactBuildPage>();
        for (int partition = 0; partition < stage.parallelism(); partition++) {
            result.add(new ArtifactBuildPage(job, pageId(job, stage.stageId(), partition),
                    stage.stageId(), partition,
                    Math.addExact(ordinalBase, partition), kind,
                    producers.isEmpty() ? allocations.get(partition).stream().map(byBytes::get).toList()
                            : List.of(), dependencies, maxAttempts));
        }
        return List.copyOf(result);
    }

    private static String pageId(String job, int stage, int partition) {
        return job + ":stage:" + stage + ":partition:" + partition;
    }

    static void requireAdmitted(AuthorizedStageGraph graph, String sourceId) {
        if (graph.terminalPurpose() != AuthorizedStageGraph.TerminalPurpose.RELATIONAL_EXPORT) {
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
