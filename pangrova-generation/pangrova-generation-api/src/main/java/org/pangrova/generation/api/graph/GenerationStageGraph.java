package org.pangrova.generation.api.graph;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.generation.api.execution.GenerationPageKind;
import org.pangrova.generation.api.graph.template.GenerationStageTemplate;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.ModelSnapshot;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Canonical durable stage topology. Metadata stores bytes and identities, never planner nodes or row data. */
public record GenerationStageGraph(int formatVersion, Purpose purpose, List<StagePlan> stages,
        List<Exchange> exchanges, List<ModelSnapshot> modelSnapshots) {
    public static final int FORMAT_VERSION = 2;
    private static final int MAX_GRAPH_ENTRIES = 4096;

    public enum Purpose { ARTIFACT_GENERATION, RELATIONAL_EXPORT }

    public GenerationStageGraph {
        if (formatVersion != FORMAT_VERSION) {
            throw new IllegalArgumentException("Durable graph requires format 2; explicitly rebuild old jobs");
        }
        Objects.requireNonNull(purpose, "purpose");
        modelSnapshots = modelSnapshots.stream().sorted(Comparator.comparing(ModelSnapshot::logicalName)).toList();
        if (modelSnapshots.stream().map(ModelSnapshot::logicalName).distinct().count() != modelSnapshots.size()) {
            throw new IllegalArgumentException("Durable graph repeats a model logical identity");
        }
        stages = stages.stream().sorted(Comparator.comparingInt(StagePlan::stageId)).toList();
        exchanges = exchanges.stream().sorted(Comparator.comparing(Exchange::exchangeId)).toList();
        validate(purpose, stages, exchanges);
    }

    /** Exact per-stage executable payload and immutable partition cohort. */
    public record StagePlan(int stageId, int parallelism, GenerationPageKind pageKind,
            String planBase64, String planSha256, GenerationStageTemplate template,
            List<String> sourceKeys, List<String> outputNames, boolean publicationOutput) {
        public StagePlan {
            Objects.requireNonNull(pageKind, "pageKind");
            sourceKeys = List.copyOf(sourceKeys);
            outputNames = List.copyOf(outputNames);
            if (stageId < 0 || parallelism < 0
                    || (parallelism == 0 && pageKind != GenerationPageKind.SOURCE_MAP)) {
                throw new IllegalArgumentException("Invalid durable stage coordinates");
            }
            decode(planBase64, planSha256, false, "stage plan");
            Objects.requireNonNull(template, "Durable stage template is absent; explicitly rebuild old jobs");
            if (template.bindingBytes().length == 0 && pageKind != GenerationPageKind.RELATIONAL_COMPUTE) {
                throw new IllegalArgumentException("Artifact terminal requires non-empty canonical bindings");
            }
            if (new HashSet<>(sourceKeys).size() != sourceKeys.size()
                    || sourceKeys.stream().anyMatch(String::isBlank)
                    || new HashSet<>(outputNames).size() != outputNames.size()
                    || outputNames.stream().anyMatch(String::isBlank)
                    || (pageKind == GenerationPageKind.RELATIONAL_COMPUTE && publicationOutput)
                    || (pageKind == GenerationPageKind.RELATIONAL_PARTITION && !publicationOutput)) {
                throw new IllegalArgumentException("Invalid durable stage source/output identity");
            }
        }

        public static StagePlan of(int stageId, int parallelism, GenerationPageKind kind,
                byte[] plan, GenerationStageTemplate template, List<String> sourceKeys, List<String> outputNames,
                boolean publicationOutput) {
            return new StagePlan(stageId, parallelism, kind, Base64.getEncoder().encodeToString(plan),
                    DigestUtils.sha256Hex(plan), template,
                    sourceKeys, outputNames, publicationOutput);
        }

        public byte[] planBytes() {
            return decode(planBase64, planSha256, false, "stage plan");
        }

        public byte[] bindingSetBytes() {
            return template.bindingBytes();
        }
    }

    /** Native exchange shape; runtime addresses and ephemeral credentials are allocated separately. */
    public record Exchange(String exchangeId, int upstreamStageId, int downstreamStageId,
            Distribution distribution, List<Integer> distributionKeys, int partitionCount,
            String batchSerde, boolean requireComplete) {
        public enum Distribution { HASH, ROUND_ROBIN, SINGLETON, BROADCAST }

        public Exchange {
            Objects.requireNonNull(distribution, "distribution");
            distributionKeys = List.copyOf(distributionKeys);
            if (exchangeId == null || exchangeId.isBlank() || upstreamStageId < 0 || downstreamStageId < 0
                    || upstreamStageId == downstreamStageId || partitionCount <= 0 || !requireComplete
                    || !"BATCH_SERDE_ARROW_BATCH".equals(batchSerde)
                    || distributionKeys.stream().anyMatch(key -> key < 0)
                    || new HashSet<>(distributionKeys).size() != distributionKeys.size()
                    || (distribution == Distribution.HASH) != !distributionKeys.isEmpty()
                    || (distribution == Distribution.SINGLETON && partitionCount != 1)) {
                throw new IllegalArgumentException("Invalid durable Native exchange contract");
            }
        }
    }

    /** Returns the exact persisted payload for the requested stage identity. */
    public StagePlan requireStage(int stageId) {
        return stages.stream().filter(stage -> stage.stageId() == stageId).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No persisted stage payload for id " + stageId));
    }

    private static byte[] decode(String encoded, String sha, boolean allowEmpty, String name) {
        if (encoded == null || sha == null || !sha.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(name + " identity is required");
        }
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(name + " is not Base64", failure);
        }
        if ((!allowEmpty && bytes.length == 0) || !DigestUtils.matchesSha256(bytes, sha)
                || !Base64.getEncoder().encodeToString(bytes).equals(encoded)) {
            throw new IllegalArgumentException(name + " is empty, noncanonical or has a different checksum");
        }
        return bytes;
    }

    private static void validate(Purpose purpose, List<StagePlan> stages, List<Exchange> exchanges) {
        if (stages.isEmpty() || stages.size() > MAX_GRAPH_ENTRIES || exchanges.size() > MAX_GRAPH_ENTRIES) {
            throw new IllegalArgumentException("Durable graph must have bounded non-empty stage metadata");
        }
        Map<Integer, StagePlan> byId = indexStages(purpose, stages);
        if (stages.stream().noneMatch(StagePlan::publicationOutput)) {
            throw new IllegalArgumentException("Durable graph has no publication terminal");
        }
        Set<String> ids = new HashSet<>();
        Map<Integer, List<Integer>> downstream = new HashMap<>();
        Map<Integer, Integer> remaining = new HashMap<>();
        byId.keySet().forEach(id -> remaining.put(id, 0));
        for (var edge : exchanges) {
            var source = byId.get(edge.upstreamStageId());
            var target = byId.get(edge.downstreamStageId());
            if (source == null || target == null || source.parallelism() == 0
                    || edge.partitionCount() != target.parallelism() || !ids.add(edge.exchangeId())
                    || source.publicationOutput()) {
                throw new IllegalArgumentException("Durable graph exchange is unbound or changes its partition cohort");
            }
            downstream.computeIfAbsent(source.stageId(), ignored -> new ArrayList<>()).add(target.stageId());
            remaining.compute(target.stageId(), (ignored, count) -> count + 1);
        }
        validateAcyclic(stages.size(), downstream, remaining);
        if (purpose == Purpose.RELATIONAL_EXPORT && stages.stream().anyMatch(stage ->
                !stage.publicationOutput() && !downstream.containsKey(stage.stageId()))) {
            throw new IllegalArgumentException("Durable relational graph has a detached compute stage");
        }
    }

    private static Map<Integer, StagePlan> indexStages(Purpose purpose, List<StagePlan> stages) {
        Map<Integer, StagePlan> byId = new HashMap<>();
        for (var stage : stages) {
            if (byId.put(stage.stageId(), stage) != null) {
                throw new IllegalArgumentException("Duplicate durable stage id");
            }
            boolean relational = stage.pageKind() == GenerationPageKind.RELATIONAL_COMPUTE
                    || stage.pageKind() == GenerationPageKind.RELATIONAL_PARTITION;
            if (relational != (purpose == Purpose.RELATIONAL_EXPORT)) {
                throw new IllegalArgumentException("Durable stage kind differs from terminal purpose");
            }
        }
        return byId;
    }

    private static void validateAcyclic(int stageCount, Map<Integer, List<Integer>> downstream,
            Map<Integer, Integer> remaining) {
        var ready = new ArrayDeque<Integer>();
        remaining.forEach((id, count) -> { if (count == 0) ready.add(id); });
        int visited = 0;
        while (!ready.isEmpty()) {
            int stage = ready.remove();
            visited++;
            for (int next : downstream.getOrDefault(stage, List.of())) {
                if (remaining.compute(next, (ignored, count) -> count - 1) == 0) ready.add(next);
            }
        }
        if (visited != stageCount) throw new IllegalArgumentException("Durable graph contains a cycle");
    }
}
