package org.pangrova.planner.analysis;

import org.pangrova.planner.api.fragment.SerializedPlanFragment;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.BatchSerde;
import org.pangrova.protocol.v2.ExchangeInputDetail;
import org.pangrova.protocol.v2.RelationDetailEnvelope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Freezes exchange schemas from actual canonical producer outputs, never presentation labels. */
final class AuthorizedStageExchangeBindings {
    private AuthorizedStageExchangeBindings() {}

    static SerializedPlanFragment bindLive(int stageId, SerializedPlanFragment consumer,
            List<StageTransfer> incoming, Map<Integer, List<byte[]>> producerPlans) {
        Fragment target = fragment(stageId, consumer);
        var edges = incoming.stream().collect(Collectors.toMap(StageTransfer::exchangeId, Function.identity()));
        requireInputs(target, edges);
        if (edges.isEmpty()) return consumer;
        Map<Rel, Rel> replacements = new IdentityHashMap<>();
        for (Rel relation : target.layout().relations()) {
            var input = input(relation);
            if (input == null) continue;
            var edge = edges.get(input.getExchangeId());
            validateInput(input, edge);
            var plans = producerPlans.get(edge.upstreamStageId());
            if (plans == null || plans.isEmpty()) {
                throw new IllegalArgumentException("Live exchange has no dispatched producer plan: "
                        + edge.exchangeId());
            }
            NamedStruct schema = null;
            for (byte[] bytes : plans) {
                var source = fragment(edge.upstreamStageId(),
                        new SerializedPlanFragment(bytes, List.of(), List.of()));
                validateProducer(source, edge);
                if (schema != null && !schema.equals(source.schema())) {
                    throw new IllegalArgumentException("Live exchange producer partitions have different schemas: "
                            + edge.exchangeId());
                }
                schema = source.schema();
            }
            if (!schema.getStruct().equals(input.getOutputSchema().getStruct())) {
                throw new IllegalArgumentException("Live exchange input type differs from producer: "
                        + edge.exchangeId() + " consumer="
                        + input.getOutputSchema().getStruct().getTypesList()
                        + " producer=" + schema.getStruct().getTypesList());
            }
            var envelope = envelope(relation).toBuilder().setExchangeInput(
                    input.toBuilder().setOutputSchema(schema)).build();
            replacements.put(relation, relation.toBuilder().setExtensionLeaf(
                    relation.getExtensionLeaf().toBuilder().setDetail(Any.pack(envelope))).build());
        }
        Rel root = SubstraitRelationLayout.replaceRelations(target.root().getInput(), replacements);
        var plan = target.plan().toBuilder().setRelations(0, target.plan().getRelations(0).toBuilder()
                .setRoot(target.root().toBuilder().setInput(root))).build();
        return new SerializedPlanFragment(plan.toByteArray(), consumer.retrievalTopKHints(),
                consumer.aiInvocationHints());
    }

    static void validate(List<AuthorizedStageGraph.SerializedStage> stages, List<StageTransfer> transfers) {
        validate(stages.stream().collect(Collectors.toMap(AuthorizedStageGraph.SerializedStage::stageId,
                AuthorizedStageGraph.SerializedStage::serializedPlan)), transfers);
    }

    static void validate(Map<Integer, SerializedPlanFragment> plans, List<StageTransfer> transfers) {
        Map<Integer, Fragment> fragments = new HashMap<>();
        plans.forEach((id, plan) -> fragments.put(id, fragment(id, plan)));
        for (var edge : transfers) {
            if (!fragments.containsKey(edge.upstreamStageId()) || !fragments.containsKey(edge.downstreamStageId())) {
                throw new IllegalArgumentException("Exchange requires both frozen stage plans: " + edge.exchangeId());
            }
        }
        var incoming = incoming(transfers);
        for (var stage : fragments.entrySet()) {
            var fragment = stage.getValue();
            var expected = incoming.getOrDefault(stage.getKey(), Map.of());
            requireInputs(fragment, expected);
            for (var edge : expected.values()) {
                var source = fragments.get(edge.upstreamStageId());
                validateProducer(source, edge);
                var input = fragment.inputs().get(edge.exchangeId());
                validateInput(input, edge);
                if (!source.schema().equals(input.getOutputSchema())) {
                    throw new IllegalArgumentException(
                            "Exchange input differs from frozen producer schema: " + edge.exchangeId());
                }
            }
        }
    }

    static List<AuthorizedStageGraph.SerializedStage> bind(
            List<AuthorizedStageGraph.SerializedStage> stages, List<StageTransfer> transfers) {
        var pending = stages.stream().collect(Collectors.toMap(
                AuthorizedStageGraph.SerializedStage::stageId, Function.identity()));
        Map<Integer, Fragment> frozen = new HashMap<>();
        List<AuthorizedStageGraph.SerializedStage> bound = new ArrayList<>();
        var incoming = incoming(transfers);
        Map<Integer, Integer> degrees = new HashMap<>();
        pending.keySet().forEach(id -> degrees.put(id, incoming.getOrDefault(id, Map.of()).size()));
        var downstream = transfers.stream().collect(Collectors.groupingBy(StageTransfer::upstreamStageId,
                Collectors.mapping(StageTransfer::downstreamStageId, Collectors.toList())));
        var ready = new TreeSet<Integer>();
        degrees.forEach((id, degree) -> { if (degree == 0) ready.add(id); });
        while (!ready.isEmpty()) {
            Integer id = ready.pollFirst();
            var stage = bindStage(pending.remove(id), incoming.getOrDefault(id, Map.of()), frozen);
            frozen.put(id, fragment(stage));
            bound.add(stage);
            for (int target : downstream.getOrDefault(id, List.of())) {
                if (degrees.compute(target, (key, degree) -> degree - 1) == 0) ready.add(target);
            }
        }
        if (!pending.isEmpty()) throw new IllegalArgumentException("Cannot freeze cyclic or incomplete exchange graph");
        return List.copyOf(bound);
    }

    private static AuthorizedStageGraph.SerializedStage bindStage(AuthorizedStageGraph.SerializedStage stage,
            Map<String, StageTransfer> incoming, Map<Integer, Fragment> sources) {
        Fragment fragment = fragment(stage);
        requireInputs(fragment, incoming);
        Map<Rel, Rel> replacements = new IdentityHashMap<>();
        for (Rel relation : fragment.layout().relations()) {
            var input = input(relation);
            if (input == null) continue;
            var edge = incoming.get(input.getExchangeId());
            Fragment source = sources.get(edge.upstreamStageId());
            validateProducer(source, edge);
            validateInput(input, edge);
            if (!source.schema().getStruct().equals(input.getOutputSchema().getStruct())) {
                throw new IllegalArgumentException("Exchange input type differs from producer: " + edge.exchangeId());
            }
            var envelope = envelope(relation).toBuilder().setExchangeInput(
                    input.toBuilder().setOutputSchema(source.schema())).build();
            replacements.put(relation, relation.toBuilder().setExtensionLeaf(
                    relation.getExtensionLeaf().toBuilder().setDetail(Any.pack(envelope))).build());
        }
        Rel root = SubstraitRelationLayout.replaceRelations(fragment.root().getInput(), replacements);
        var plan = fragment.plan().toBuilder().setRelations(0, fragment.plan().getRelations(0).toBuilder()
                .setRoot(fragment.root().toBuilder().setInput(root))).build();
        var serialized = new SerializedPlanFragment(plan.toByteArray(), stage.serializedPlan().retrievalTopKHints(),
                stage.serializedPlan().aiInvocationHints());
        return new AuthorizedStageGraph.SerializedStage(stage.stageId(), stage.parallelism(), stage.taskType(),
                serialized, stage.sources(), stage.outputNames(), stage.resultColumnModels(),
                stage.resultColumnTypes(),
                stage.estimatedShuffleBytes(), stage.estimatedShuffleRows());
    }

    private static Map<Integer, Map<String, StageTransfer>> incoming(List<StageTransfer> transfers) {
        return transfers.stream().collect(Collectors.groupingBy(StageTransfer::downstreamStageId,
                Collectors.toMap(StageTransfer::exchangeId, Function.identity())));
    }

    private static Fragment fragment(AuthorizedStageGraph.SerializedStage stage) {
        return fragment(stage.stageId(), stage.serializedPlan());
    }

    private static Fragment fragment(int stageId, SerializedPlanFragment serialized) {
        Plan plan;
        try {
            plan = org.pangrova.protocol.substrait.SubstraitPlanCodec.parse(serialized.planBytes());
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Stage requires canonical serialized plan: " + stageId, failure);
        }
        if (plan.getRelationsCount() != 1 || !plan.getRelations(0).hasRoot()) {
            throw new IllegalArgumentException("Stage requires one canonical root: " + stageId);
        }
        RelRoot root = plan.getRelations(0).getRoot();
        SubstraitRelationLayout.rootSchema(root);
        var layout = SubstraitRelationLayout.describeTree(root.getInput());
        Map<String, ExchangeInputDetail> inputs = new HashMap<>();
        for (Rel relation : layout.relations()) {
            var input = input(relation);
            if (input != null && inputs.putIfAbsent(input.getExchangeId(), input) != null) {
                throw new IllegalArgumentException("Duplicate exchange input: " + input.getExchangeId());
            }
        }
        return new Fragment(plan, root, layout, layout.layout(root.getInput()).outputSchema(), Map.copyOf(inputs));
    }

    private static void requireInputs(Fragment fragment, Map<String, StageTransfer> expected) {
        if (!fragment.inputs().keySet().equals(expected.keySet())) {
            throw new IllegalArgumentException("Stage exchange inputs do not match its frozen graph edges");
        }
    }

    private static ExchangeInputDetail input(Rel relation) {
        if (!relation.hasExtensionLeaf()) return null;
        var envelope = envelope(relation);
        return envelope.hasExchangeInput() ? envelope.getExchangeInput() : null;
    }

    private static RelationDetailEnvelope envelope(Rel relation) {
        try {
            return relation.getExtensionLeaf().getDetail().unpack(RelationDetailEnvelope.class);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Exchange requires its canonical typed envelope", failure);
        }
    }

    private static void validateInput(ExchangeInputDetail input, StageTransfer edge) {
        if (input.getPlanNodeId().isBlank() || input.getUpstreamStageId() != edge.upstreamStageId()
                || input.getDownstreamStageId() != edge.downstreamStageId()
                || input.getPartitionCount() != edge.partitionCount()
                || input.getBatchSerde() != BatchSerde.BATCH_SERDE_ARROW_BATCH
                || !input.getRequireComplete() || !edge.requireComplete()
                || !input.getDistribution().name().equals("EXCHANGE_DISTRIBUTION_" + edge.shuffleType().name())) {
            throw new IllegalArgumentException("Exchange input identity differs from graph edge: " + edge.exchangeId());
        }
    }

    private static void validateProducer(Fragment source, StageTransfer edge) {
        if (source == null) throw new IllegalArgumentException(
                "Exchange requires a frozen producer: " + edge.exchangeId());
        Rel relation = source.root().getInput();
        if (!relation.hasExchange() || relation.getExchange().getPartitionCount() != edge.partitionCount()) {
            throw new IllegalArgumentException("Producer requires its bound terminal exchange: " + edge.exchangeId());
        }
        var exchange = relation.getExchange();
        boolean matches = switch (edge.shuffleType()) {
            case SINGLETON -> exchange.hasRoundRobin() && exchange.getRoundRobin().getExact()
                    && edge.partitionCount() == 1 && edge.distributionKeys().isEmpty();
            case ROUND_ROBIN -> exchange.hasRoundRobin() && !exchange.getRoundRobin().getExact()
                    && edge.distributionKeys().isEmpty();
            case BROADCAST -> exchange.hasBroadcast() && edge.distributionKeys().isEmpty();
            case HASH -> exchange.hasScatterByFields() && hashKeys(exchange).equals(edge.distributionKeys())
                    && !edge.distributionKeys().isEmpty();
            case RANGE, ANY -> false;
        };
        if (!matches) throw new IllegalArgumentException(
                "Producer distribution differs from graph edge: " + edge.exchangeId());
    }

    private static List<Integer> hashKeys(io.substrait.proto.ExchangeRel exchange) {
        return exchange.getScatterByFields().getFieldsList().stream().map(field -> {
            if (!field.hasRootReference() || !field.hasDirectReference()
                    || !field.getDirectReference().hasStructField()
                    || field.getDirectReference().getStructField().hasChild()) {
                throw new IllegalArgumentException("Frozen hash distribution requires exact top-level root fields");
            }
            return field.getDirectReference().getStructField().getField();
        }).toList();
    }

    private record Fragment(Plan plan, RelRoot root, SubstraitRelationLayout.LayoutTree layout,
                            NamedStruct schema, Map<String, ExchangeInputDetail> inputs) {}
}
