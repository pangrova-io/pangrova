package org.pangrova.planner.core.analysis;

import org.pangrova.planner.api.fragment.SerializedPlanFragment;

import org.pangrova.planner.api.result.ResultColumnModel;

import org.pangrova.common.util.DigestUtils;
import org.pangrova.common.api.annotation.Internal;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.ResultColumnLogicalType;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Immutable serialized execution graph produced from an already authorized relational plan. */
@Internal
public record AuthorizedStageGraph(TerminalPurpose terminalPurpose,
        List<SerializedStage> stages, List<StageTransfer> transfers) {
    /** Same relational engine, with an explicit result-collection or immutable export terminal. */
    public enum TerminalPurpose { QUERY_RESULT, RELATIONAL_EXPORT }

    /** Requires one complete acyclic graph, with no unbound exchange or duplicate stage. */
    public AuthorizedStageGraph {
        Objects.requireNonNull(terminalPurpose, "terminalPurpose");
        stages = List.copyOf(stages.stream().sorted(Comparator.comparingInt(SerializedStage::stageId)).toList());
        transfers = List.copyOf(transfers.stream().sorted(Comparator.comparingInt(StageTransfer::upstreamStageId)
                .thenComparingInt(StageTransfer::downstreamStageId).thenComparing(StageTransfer::exchangeId)).toList());
        validate(stages, transfers);
        AuthorizedStageExchangeBindings.validate(stages, transfers);
    }

    /** Stage payload has no planner framework node and can be durably replayed without SQL recompilation. */
    @Internal
    public record SerializedStage(int stageId, int parallelism, String taskType,
            SerializedPlanFragment serializedPlan, List<SplitSourceDescriptor> sources,
            List<String> outputNames, List<ResultColumnModel> resultColumnModels,
            List<ResultColumnLogicalType> resultColumnTypes,
            long estimatedShuffleBytes, long estimatedShuffleRows) implements StagePlanAccess {
        /** Copies metadata and requires explicit stage role, field names and canonical non-empty plan bytes. */
        public SerializedStage {
            Objects.requireNonNull(serializedPlan, "serializedPlan");
            sources = List.copyOf(sources);
            outputNames = List.copyOf(outputNames);
            resultColumnModels = List.copyOf(resultColumnModels);
            resultColumnTypes = List.copyOf(resultColumnTypes.stream()
                    .sorted(Comparator.comparingInt(ResultColumnLogicalType::getColumnIndex)).toList());
            Set<Integer> typedColumns = new HashSet<>();
            for (var column : resultColumnTypes) {
                VectorLogicalType.requireStorageDimension(column.getLogicalType());
                if (column.getColumnIndex() < 0 || column.getColumnIndex() >= outputNames.size()
                        || !typedColumns.add(column.getColumnIndex())
                        || !column.getUnknownFields().asMap().isEmpty()
                        || resultColumnModels.stream().anyMatch(
                                model -> model.columnIndex() == column.getColumnIndex())) {
                    throw new IllegalArgumentException("Invalid or conflicting declared result column identity");
                }
            }
            if (stageId < 0 || parallelism <= 0 || serializedPlan.planBytes().length == 0
                    || !("OUTPUT".equals(taskType) || "COMPUTE".equals(taskType))
                    || outputNames.stream().anyMatch(String::isBlank)
                    || new HashSet<>(outputNames).size() != outputNames.size()) {
                throw new IllegalArgumentException("Invalid authorized stage payload");
            }
        }

        @Override
        public List<ResultColumnLogicalType> resultColumnTypes() {
            return List.copyOf(resultColumnTypes);
        }

        @Override
        public java.util.Optional<org.pangrova.planner.api.fragment.StageCoordinates> frozenCoordinates() {
            return java.util.Optional.of(new org.pangrova.planner.api.fragment.StageCoordinates(stageId, parallelism, taskType));
        }

        @Override
        public Mode mode() {
            return Mode.DURABLE_REPLAY;
        }

        @Override
        public List<SplitSourceDescriptor> splitSources() {
            return sources;
        }

        @Override
        public List<String> queryOutputColumns() {
            return outputNames;
        }

        @Override
        public List<String> fieldNames() {
            return outputNames;
        }

        @Override
        public SerializedPlanFragment toSerializedPlan(SqlCompiler compiler, List<String> columnNames) {
            if (!outputNames.equals(columnNames)) {
                throw new IllegalArgumentException("Durable stage output identity cannot be rebound");
            }
            return serializedPlan;
        }

        @Override
        public PlanFragment requireLiveFragment() {
            throw new IllegalStateException("Durable stage has no live planner capability");
        }

        /** Identity binds pinned allocations to this exact authorized stage, not merely its numeric id. */
        public String sha256() {
            try {
                var bytes = new ByteArrayOutputStream();
                try (var output = new DataOutputStream(bytes)) {
                    writeStage(output, this);
                }
                return DigestUtils.sha256Hex(bytes.toByteArray());
            } catch (IOException failure) {
                throw new IllegalStateException("Cannot encode authorized stage identity", failure);
            }
        }
    }

    /** Strictly serializes each protected fragment, retaining exactly the existing exchange graph. */
    public static AuthorizedStageGraph compile(FragmentedPlan graph, SqlCompiler compiler,
            List<String> outputNames, TerminalPurpose terminalPurpose) {
        Objects.requireNonNull(graph, "graph");
        Objects.requireNonNull(compiler, "compiler");
        List<SerializedStage> stages = new ArrayList<>();
        for (var stage : graph.stages()) {
            var fragment = stage.fragment();
            List<String> names = "OUTPUT".equals(stage.taskType()) ? List.copyOf(outputNames) : fragment.fieldNames();
            stages.add(new SerializedStage(stage.stageId(), stage.parallelism(), stage.taskType(),
                    fragment.toSerializedPlan(compiler, names), fragment.splitSources(), names,
                    fragment.resultColumnModels(), fragment.resultColumnTypes(),
                    fragment.estimatedShuffleBytes(), fragment.estimatedShuffleRows()));
        }
        return new AuthorizedStageGraph(terminalPurpose,
                AuthorizedStageExchangeBindings.bind(stages, graph.transfers()), graph.transfers());
    }

    /**
     * Verifies actual frozen wire schemas during replay; never repairs or recompiles persisted plans.
     * @param plans exact serialized canonical plan per stage
     * @param transfers frozen exchange edges
     * @throws IllegalArgumentException for missing plans or inconsistent canonical wire contracts
     */
    public static void validateExchangeContracts(Map<Integer, SerializedPlanFragment> plans,
            List<StageTransfer> transfers) {
        AuthorizedStageExchangeBindings.validate(Map.copyOf(plans), List.copyOf(transfers));
    }

    /** Binds a live consumer to the canonical schemas of its dispatched producer plans. */
    public static SerializedPlanFragment bindLiveExchangeInputs(int stageId, SerializedPlanFragment consumer,
            List<StageTransfer> incoming, Map<Integer, List<byte[]>> producerPlans) {
        return AuthorizedStageExchangeBindings.bindLive(stageId, consumer, incoming, producerPlans);
    }

    /** Returns a stable digest covering stage payloads, scheduling coordinates, exact sources and exchanges. */
    public String sha256() {
        try {
            var buffer = new ByteArrayOutputStream();
            try (var output = new DataOutputStream(buffer)) {
                writeText(output, terminalPurpose.name());
                output.writeInt(stages.size());
                for (var stage : stages) writeStage(output, stage);
                output.writeInt(transfers.size());
                for (var transfer : transfers) writeTransfer(output, transfer);
            }
            return DigestUtils.sha256Hex(buffer.toByteArray());
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot encode authorized graph identity", failure);
        }
    }

    private static void writeStage(DataOutputStream output, SerializedStage stage) throws IOException {
        output.writeInt(stage.stageId());
        output.writeInt(stage.parallelism());
        writeText(output, stage.taskType());
        byte[] plan = stage.serializedPlan().planBytes();
        output.writeInt(plan.length);
        output.write(plan);
        output.writeInt(stage.sources().size());
        for (var source : stage.sources()) {
            writeText(output, source.qualifiedName());
            output.writeInt(source.tableColumns().size());
            for (String column : source.tableColumns()) writeText(output, column);
        }
        output.writeInt(stage.outputNames().size());
        for (String name : stage.outputNames()) writeText(output, name);
        output.writeInt(stage.resultColumnModels().size());
        for (var model : stage.resultColumnModels()) {
            output.writeInt(model.columnIndex());
            writeText(output, model.modelName());
        }
        output.writeInt(stage.resultColumnTypes().size());
        for (var type : stage.resultColumnTypes()) {
            byte[] bytes = type.toByteArray();
            output.writeInt(bytes.length);
            output.write(bytes);
        }
        output.writeInt(stage.serializedPlan().retrievalTopKHints().size());
        for (var hint : stage.serializedPlan().retrievalTopKHints()) {
            writeText(output, hint.kind().name());
            writeText(output, hint.sourceKey());
            output.writeInt(hint.scoreFieldIndex());
            output.writeInt(hint.candidateLimit());
            writeText(output, hint.scoreDirection().name());
            output.writeInt(hint.stableKeyFieldIndexes().size());
            for (int key : hint.stableKeyFieldIndexes()) output.writeInt(key);
            byte[] schema = hint.candidateSchema();
            output.writeInt(schema.length);
            output.write(schema);
        }
        output.writeInt(stage.serializedPlan().aiInvocationHints().size());
        for (var hint : stage.serializedPlan().aiInvocationHints()) {
            writeText(output, hint.invocationId());
            writeText(output, hint.functionName());
            writeText(output, hint.modelName());
            writeText(output, hint.operation().name());
            output.writeBoolean(hint.mediaInput());
        }
    }

    private static void writeTransfer(DataOutputStream output, StageTransfer edge) throws IOException {
        output.writeInt(edge.upstreamStageId());
        output.writeInt(edge.downstreamStageId());
        writeText(output, edge.exchangeId());
        writeText(output, edge.shuffleType().name());
        output.writeInt(edge.partitionCount());
        output.writeInt(edge.batchSerde().getNumber());
        output.writeBoolean(edge.requireComplete());
        output.writeInt(edge.distributionKeys().size());
        for (int key : edge.distributionKeys()) output.writeInt(key);
    }

    private static void writeText(DataOutputStream output, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void validate(List<SerializedStage> stages, List<StageTransfer> edges) {
        Map<Integer, SerializedStage> byId = new HashMap<>();
        for (var stage : stages) {
            if (byId.put(stage.stageId(), stage) != null) throw new IllegalArgumentException("Duplicate stage id");
        }
        if (stages.isEmpty() || stages.stream().filter(stage -> "OUTPUT".equals(stage.taskType())).count() != 1) {
            throw new IllegalArgumentException("Authorized graph requires one terminal output stage");
        }
        Set<String> exchanges = new HashSet<>();
        Map<Integer, List<Integer>> downstream = new HashMap<>();
        Map<Integer, Integer> inDegree = new HashMap<>();
        byId.keySet().forEach(id -> inDegree.put(id, 0));
        for (var edge : edges) {
            var producer = byId.get(edge.upstreamStageId());
            var consumer = byId.get(edge.downstreamStageId());
            if (producer == null || consumer == null || !exchanges.add(edge.exchangeId())
                    || producer.stageId() == consumer.stageId() || !edge.requireComplete()
                    || edge.partitionCount() != consumer.parallelism() || "OUTPUT".equals(producer.taskType())) {
                throw new IllegalArgumentException("Authorized graph contains an invalid exchange edge");
            }
            downstream.computeIfAbsent(producer.stageId(), ignored -> new ArrayList<>()).add(consumer.stageId());
            inDegree.compute(consumer.stageId(), (ignored, count) -> count + 1);
        }
        for (var stage : stages) {
            if (!"OUTPUT".equals(stage.taskType()) && !downstream.containsKey(stage.stageId())) {
                throw new IllegalArgumentException("Authorized graph contains a detached compute stage");
            }
        }
        var ready = new ArrayDeque<Integer>();
        inDegree.forEach((id, count) -> { if (count == 0) ready.add(id); });
        int visited = 0;
        while (!ready.isEmpty()) {
            int id = ready.remove();
            visited++;
            for (int target : downstream.getOrDefault(id, List.of())) {
                if (inDegree.compute(target, (ignored, count) -> count - 1) == 0) ready.add(target);
            }
        }
        if (visited != stages.size()) throw new IllegalArgumentException("Authorized graph contains a cycle");
    }
}
