package org.pangrova.protocol.generation;

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.substrait.proto.FilterRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.Type;
import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.NamedExpressionArgument;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.TableFunctionDetail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Inserts exact published generated-value reads below already compiled policy expressions. */
public final class GeneratedValuesOverlayPlanBinder {
    private static final String SOURCE_OBJECT = "$pangrova$source_object_id";
    private static final String POSITION = "$pangrova$row_position";

    private GeneratedValuesOverlayPlanBinder() {}

    /** Describes a read-local replacement; the unsigned artifact template contains no physical objects. */
    public record ReadOverlay(int readOrdinal, List<String> table, String targetColumn, OperatorBinding binding) {
        public ReadOverlay {
            table = List.copyOf(table);
            Objects.requireNonNull(targetColumn, "targetColumn");
            Objects.requireNonNull(binding, "binding");
            if (readOrdinal < 0 || table.size() != 3
                    || table.stream().anyMatch(String::isBlank) || targetColumn.isBlank()
                    || binding.getPlanNodeId().isBlank() || binding.getArtifactsCount() != 1
                    || !binding.getCapability().equals(BuiltInCapabilityCatalog.generatedValuesOverlayPlanRef())) {
                throw new IllegalArgumentException("Generated overlay requires an exact read and template");
            }
        }
    }

    /** Lists actual serialized reads, preserving order and their full schema/emit contracts. */
    public static List<ReadRel> reads(byte[] planBytes) {
        List<ReadRel> result = new ArrayList<>();
        rewrite(parse(planBytes), read -> {
            result.add(read);
            return Rel.newBuilder().setRead(read).build();
        });
        return List.copyOf(result);
    }

    /** Replaces only requested physical read outputs, preserving every downstream ordinal and expression. */
    public static BoundExecutionPlan bind(byte[] planBytes, List<ReadOverlay> overlays) {
        if (overlays.isEmpty()) return new BoundExecutionPlan(planBytes, List.of());
        Map<Integer, List<ReadOverlay>> byRead = new HashMap<>();
        for (var overlay : overlays) {
            var values = byRead.computeIfAbsent(overlay.readOrdinal(), ignored -> new ArrayList<>());
            if (values.stream().anyMatch(value -> value.targetColumn().equals(overlay.targetColumn()))) {
                throw new IllegalArgumentException("Generated overlay target is duplicated");
            }
            values.add(overlay);
        }
        List<OperatorBinding> bindings = new ArrayList<>();
        var ordinal = new java.util.concurrent.atomic.AtomicInteger();
        Plan rewritten = rewrite(parse(planBytes),
                read -> bindRead(read, byRead.get(ordinal.getAndIncrement()), bindings));
        if (bindings.size() != overlays.size()) {
            throw new IllegalArgumentException("Generated overlays must match exactly one serialized read each");
        }
        return new BoundExecutionPlan(rewritten.toByteArray(), bindings);
    }

    private static Rel bindRead(ReadRel read, List<ReadOverlay> overlays,
                                List<OperatorBinding> bindings) {
        if (overlays == null) return Rel.newBuilder().setRead(read).build();
        requirePhysicalRead(read, overlays);
        ReadLayout layout = prepareRead(read, overlays);
        NamedStruct schema = read.getBaseSchema();
        Map<Integer, ReadOverlay> targets = layout.targets();
        List<Integer> kept = layout.kept();
        Rel relation = layout.relation();
        NamedStruct currentSchema = layout.schema();
        Map<Integer, Integer> replacements = new HashMap<>();
        for (var entry : targets.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList()) {
            String nodeId = entry.getValue().binding().getPlanNodeId();
            replacements.put(entry.getKey(), currentSchema.getStruct().getTypesCount());
            currentSchema = currentSchema.toBuilder().addNames(
                            SubstraitRelationLayout.topLevelNames(schema).get(entry.getKey()))
                    .setStruct(currentSchema.getStruct().toBuilder()
                            .addTypes(schema.getStruct().getTypes(entry.getKey())))
                    .build();
            relation = overlay(relation, currentSchema, nodeId, kept.size(),
                    entry.getValue().binding().getCapability());
            bindings.add(entry.getValue().binding());
        }
        var restored = RelCommon.Emit.newBuilder();
        for (int field = 0; field < schema.getStruct().getTypesCount(); field++) {
            restored.addOutputMapping(targets.containsKey(field) ? replacements.get(field) : kept.indexOf(field));
        }
        relation = Rel.newBuilder().setProject(ProjectRel.newBuilder().setInput(relation)
                .setCommon(RelCommon.newBuilder().setEmit(restored))).build();
        if (read.hasFilter()) {
            relation = Rel.newBuilder().setFilter(FilterRel.newBuilder()
                    .setInput(relation).setCondition(read.getFilter())).build();
        }
        var originalEmit = RelCommon.Emit.newBuilder().addAllOutputMapping(layout.original());
        return Rel.newBuilder().setProject(ProjectRel.newBuilder().setInput(relation)
                .setCommon(RelCommon.newBuilder().setEmit(originalEmit))).build();
    }

    private static ReadLayout prepareRead(ReadRel read, List<ReadOverlay> overlays) {
        NamedStruct schema = read.getBaseSchema();
        List<String> names = SubstraitRelationLayout.topLevelNames(schema);
        if (names.contains(SOURCE_OBJECT) || names.contains(POSITION)) {
            throw new IllegalArgumentException("Generated physical read schema is invalid");
        }
        int fields = schema.getStruct().getTypesCount();
        List<Integer> original = SubstraitRelationLayout.outputMapping(read.getCommon(), fields);
        Map<Integer, ReadOverlay> targets = targetFields(schema, overlays);
        List<Integer> kept = java.util.stream.IntStream.range(0, fields).boxed()
                .filter(field -> !targets.containsKey(field)).toList();
        var extended = schema.toBuilder().addNames(SOURCE_OBJECT).addNames(POSITION)
                .setStruct(schema.getStruct().toBuilder()
                        .addTypes(Type.newBuilder().setString(Type.String.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)))
                        .addTypes(Type.newBuilder().setI64(Type.I64.newBuilder()
                                .setNullability(Type.Nullability.NULLABILITY_REQUIRED))));
        var emitted = RelCommon.Emit.newBuilder().addAllOutputMapping(kept)
                .addOutputMapping(fields).addOutputMapping(fields + 1);
        Rel relation = Rel.newBuilder().setRead(read.toBuilder().setBaseSchema(extended)
                .clearFilter().clearBestEffortFilter()
                .setCommon(read.getCommon().toBuilder().setEmit(emitted))).build();
        NamedStruct currentSchema = SubstraitRelationLayout.projectSchema(
                extended.build(), emitted.getOutputMappingList());

        return new ReadLayout(original, targets, kept, relation, currentSchema);
    }

    private record ReadLayout(List<Integer> original, Map<Integer, ReadOverlay> targets, List<Integer> kept,
                              Rel relation, NamedStruct schema) {}

    private static void requirePhysicalRead(ReadRel read, List<ReadOverlay> overlays) {
        if (!read.hasNamedTable() || !read.hasBaseSchema() || read.hasProjection()
                || overlays.stream().anyMatch(value -> !value.table().equals(read.getNamedTable().getNamesList()))) {
            throw new IllegalArgumentException("Generated overlay requires a complete named physical read");
        }
    }

    private static Map<Integer, ReadOverlay> targetFields(NamedStruct schema, List<ReadOverlay> overlays) {
        Map<Integer, ReadOverlay> targets = new HashMap<>();
        List<String> names = SubstraitRelationLayout.topLevelNames(schema);
        for (var overlay : overlays) {
            int field = names.indexOf(overlay.targetColumn());
            if (field < 0 || targets.put(field, overlay) != null
                    || names.lastIndexOf(overlay.targetColumn()) != field) {
                throw new IllegalArgumentException("Generated target is absent, duplicated or not a read output");
            }
            Type type = schema.getStruct().getTypes(field);
            if (!type.hasList() || !type.getList().getType().hasFp32()) {
                throw new IllegalArgumentException("Generated target must be a compiled FLOAT vector");
            }
        }
        return targets;
    }

    private static Rel overlay(Rel input, NamedStruct output, String nodeId, int hiddenStart,
                               org.pangrova.protocol.v2.CapabilityRef capability) {
        var invocation = CapabilityInvocation.newBuilder().setCapability(capability)
                .addArguments(argument("source_object_id", hiddenStart))
                .addArguments(argument("row_position", hiddenStart + 1));
        var detail = TableFunctionDetail.newBuilder().setPlanNodeId(nodeId)
                .setInvocation(invocation).setOutputSchema(output);
        return Rel.newBuilder().setExtensionSingle(io.substrait.proto.ExtensionSingleRel.newBuilder()
                .setInput(input).setDetail(Any.pack(RelationDetailEnvelope.newBuilder()
                        .setSchemaVersion(1).setTableFunction(detail).build()))).build();
    }

    private static NamedExpressionArgument argument(String name, int field) {
        return NamedExpressionArgument.newBuilder().setName(name)
                .setExpression(org.pangrova.protocol.substrait.SubstraitRelationLayout
                        .rootFieldReference(field)).build();
    }

    private static Plan parse(byte[] bytes) {
        try {
            return Plan.parseFrom(bytes);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Generated query plan is malformed", failure);
        }
    }

    private static Plan rewrite(Plan plan, java.util.function.Function<ReadRel, Rel> rewriter) {
        var result = plan.toBuilder();
        for (int index = 0; index < plan.getRelationsCount(); index++) {
            var relation = plan.getRelations(index);
            if (!relation.hasRoot()) throw new IllegalArgumentException("Generated query requires rooted fragments");
            result.setRelations(index, relation.toBuilder().setRoot(relation.getRoot().toBuilder()
                    .setInput(rewriteRel(relation.getRoot().getInput(), rewriter))));
        }
        return result.build();
    }

    private static Rel rewriteRel(Rel relation, java.util.function.Function<ReadRel, Rel> rewriter) {
        if (relation.hasRead()) return rewriter.apply(relation.getRead());
        FieldDescriptor active = relation.getDescriptorForType()
                .findFieldByNumber(relation.getRelTypeCase().getNumber());
        if (active == null || !relation.hasField(active)) {
            throw new IllegalArgumentException("Generated query contains an invalid relation");
        }
        Message payload = (Message) relation.getField(active);
        var changed = payload.toBuilder();
        for (var field : payload.getDescriptorForType().getFields()) {
            if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    && field.getMessageType().getFullName().equals("substrait.Rel")) {
                rewriteChild(payload, changed, field, rewriter);
            }
        }
        return relation.toBuilder().setField(active, changed.build()).build();
    }

    private static void rewriteChild(Message payload, Message.Builder changed, FieldDescriptor field,
                                     java.util.function.Function<ReadRel, Rel> rewriter) {
        if (field.isRepeated()) {
            for (int index = 0; index < payload.getRepeatedFieldCount(field); index++) {
                changed.setRepeatedField(field, index,
                        rewriteRel((Rel) payload.getRepeatedField(field, index), rewriter));
            }
        } else if (payload.hasField(field)) {
            changed.setField(field, rewriteRel((Rel) payload.getField(field), rewriter));
        }
    }
}
