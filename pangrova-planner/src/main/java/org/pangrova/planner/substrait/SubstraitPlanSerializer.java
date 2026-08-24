package org.pangrova.planner.substrait;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.ExtensionCollector;
import io.substrait.plan.ImmutablePlan;
import io.substrait.plan.ImmutableRoot;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Join;
import io.substrait.relation.Rel;
import org.pangrova.common.exception.PangrovaErrorCode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Serializes/deserializes Substrait Plan to/from Protobuf bytes.
 *
 * <p>Serialize uses the SDK's PlanProtoConverter with optional post-processing
 * for non-equi join CrossRel nodes that require expression and type fields
 * not supported by the substrait-java SDK's Cross class.</p>
 *
 * <p>Deserialize uses proto-level parsing to avoid ANTLR version conflict
 * in ProtoPlanConverter (Substrait SDK 0.23.0 bundles ANTLR 4.9.2 ATN v3,
 * incompatible with our ANTLR runtime 4.13.1 ATN v4).</p>
 */
public final class SubstraitPlanSerializer {

    private final PlanProtoConverter protoConverter = new PlanProtoConverter();

    /** Serialize a Substrait Plan to Protobuf bytes. */
    public byte[] serialize(Plan plan) {
        return serialize(plan, List.of());
    }

    /**
     * Serialize a Substrait Plan to Protobuf bytes, injecting expression and type
     * fields into CrossRel messages for non-equi joins.
     *
     * <p>The substrait-java SDK's Cross class does not expose expression or type fields.
     * When non-equi join details are present, this method post-processes the serialized
     * proto to inject the condition expression (field 4) and join type (field 5) into
     * CrossRel messages so the native engine can parse them correctly.</p>
     */
    public byte[] serialize(Plan plan, List<CrossJoinDetail> crossJoinDetails) {
        try {
            io.substrait.proto.Plan.Builder protoBuilder = protoConverter.toProto(plan).toBuilder();
            SubstraitProtocolVersion.stamp(protoBuilder);
            io.substrait.proto.Plan protoPlan = protoBuilder.build();
            if (crossJoinDetails.isEmpty()) {
                return protoPlan.toByteArray();
            }
            return injectCrossRelFields(protoPlan, crossJoinDetails);
        } catch (Exception e) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Failed to serialize Substrait Plan: " + e.getMessage(), e);
        }
    }

    /** Deserialize Protobuf bytes to a Substrait Plan. */
    public Plan deserialize(byte[] bytes) {
        io.substrait.proto.Plan protoPlan = parseProto(bytes);
        return convertFromProto(protoPlan);
    }

    /**
     * Injects expression and type fields into CrossRel messages in the serialized plan.
     * Walks the proto tree, finds CrossRel nodes, and appends the expression and type
     * field bytes to each CrossRel in order of appearance (matching crossJoinDetails order).
     */
    private byte[] injectCrossRelFields(io.substrait.proto.Plan protoPlan,
                                         List<CrossJoinDetail> details) throws IOException {
        ExtensionCollector collector = new ExtensionCollector();
        ExpressionProtoConverter exprConverter = new ExpressionProtoConverter(collector, null);

        List<byte[]> extraFieldBytes = details.stream().map(detail -> {
            try {
                return buildCrossRelExtraFields(detail, exprConverter);
            } catch (IOException e) {
                throw new SubstraitConversionException(
                        PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                        "Failed to serialize CrossRel expression: " + e.getMessage(), e);
            }
        }).toList();

        return rebuildPlanWithCrossRelFields(protoPlan, extraFieldBytes);
    }

    /**
     * Builds the extra protobuf field bytes (expression=4, type=5) for a CrossRel.
     */
    private byte[] buildCrossRelExtraFields(CrossJoinDetail detail,
                                             ExpressionProtoConverter exprConverter) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
        CodedOutputStream cos = CodedOutputStream.newInstance(baos);

        // Field 4: Expression (message type, wire type = 2 = length-delimited)
        io.substrait.proto.Expression exprProto = detail.condition().accept(exprConverter);
        byte[] exprBytes = exprProto.toByteArray();
        cos.writeTag(4, 2);
        cos.writeUInt32NoTag(exprBytes.length);
        cos.writeRawBytes(exprBytes);

        // Field 5: JoinType (enum type, wire type = 0 = varint)
        int joinTypeValue = mapJoinTypeToProto(detail.joinType());
        cos.writeTag(5, 0);
        cos.writeUInt32NoTag(joinTypeValue);

        cos.flush();
        return baos.toByteArray();
    }

    /**
     * Rebuilds the plan proto, appending extra field bytes to each CrossRel message.
     * CrossRel messages are matched to details in order of appearance (DFS traversal).
     */
    byte[] rebuildPlanWithCrossRelFields(io.substrait.proto.Plan protoPlan,
                                        List<byte[]> extraFieldBytes) throws IOException {
        int[] detailIndex = {0};
        io.substrait.proto.Plan.Builder planBuilder = protoPlan.toBuilder();
        for (int i = 0; i < planBuilder.getRelationsCount(); i++) {
            io.substrait.proto.PlanRel planRel = planBuilder.getRelations(i);
            if (planRel.hasRoot()) {
                io.substrait.proto.RelRoot root = planRel.getRoot();
                io.substrait.proto.Rel newRel = patchCrossRels(
                        root.getInput(), extraFieldBytes, detailIndex);
                planBuilder.setRelations(i, planRel.toBuilder()
                        .setRoot(root.toBuilder().setInput(newRel).build())
                        .build());
            }
        }
        return planBuilder.build().toByteArray();
    }

    /**
     * Recursively walks the Rel tree and patches CrossRel messages with extra fields.
     */
    private io.substrait.proto.Rel patchCrossRels(io.substrait.proto.Rel rel,
                                                    List<byte[]> extraFieldBytes,
                                                    int[] detailIndex) throws IOException {
        return switch (rel.getRelTypeCase()) {
            case CROSS -> patchCrossNode(rel, extraFieldBytes, detailIndex);
            case JOIN -> patchJoinNode(rel, extraFieldBytes, detailIndex);
            case PROJECT, FILTER, SORT, FETCH, AGGREGATE, EXTENSION_SINGLE ->
                    patchSingleInputNode(rel, extraFieldBytes, detailIndex);
            case EXTENSION_MULTI, SET -> patchMultiInputNode(rel, extraFieldBytes, detailIndex);
            default -> rel;
        };
    }

    private io.substrait.proto.Rel patchCrossNode(io.substrait.proto.Rel rel,
                                                    List<byte[]> extraFieldBytes,
                                                    int[] detailIndex) throws IOException {
        io.substrait.proto.CrossRel original = rel.getCross();
        io.substrait.proto.Rel patchedLeft = patchCrossRels(
                original.getLeft(), extraFieldBytes, detailIndex);
        io.substrait.proto.Rel patchedRight = patchCrossRels(
                original.getRight(), extraFieldBytes, detailIndex);
        io.substrait.proto.CrossRel rebuilt = original.toBuilder()
                .setLeft(patchedLeft).setRight(patchedRight).build();
        if (detailIndex[0] < extraFieldBytes.size()) {
            byte[] extra = extraFieldBytes.get(detailIndex[0]++);
            rebuilt = appendFieldsToCrossRel(rebuilt, extra);
        }
        return io.substrait.proto.Rel.newBuilder().setCross(rebuilt).build();
    }

    private io.substrait.proto.Rel patchJoinNode(io.substrait.proto.Rel rel,
                                                   List<byte[]> extraFieldBytes,
                                                   int[] detailIndex) throws IOException {
        var join = rel.getJoin();
        io.substrait.proto.Rel patchedLeft = patchCrossRels(
                join.getLeft(), extraFieldBytes, detailIndex);
        io.substrait.proto.Rel patchedRight = patchCrossRels(
                join.getRight(), extraFieldBytes, detailIndex);
        return rel.toBuilder().setJoin(
                join.toBuilder().setLeft(patchedLeft)
                        .setRight(patchedRight)).build();
    }

    private io.substrait.proto.Rel patchSingleInputNode(io.substrait.proto.Rel rel,
                                                          List<byte[]> extraFieldBytes,
                                                          int[] detailIndex) throws IOException {
        return switch (rel.getRelTypeCase()) {
            case PROJECT -> {
                var proj = rel.getProject();
                io.substrait.proto.Rel patched = patchCrossRels(
                        proj.getInput(), extraFieldBytes, detailIndex);
                yield rel.toBuilder().setProject(
                        proj.toBuilder().setInput(patched)).build();
            }
            case FILTER -> {
                var filter = rel.getFilter();
                io.substrait.proto.Rel patched = patchCrossRels(
                        filter.getInput(), extraFieldBytes, detailIndex);
                yield rel.toBuilder().setFilter(
                        filter.toBuilder().setInput(patched)).build();
            }
            case SORT -> {
                var sort = rel.getSort();
                io.substrait.proto.Rel patched = patchCrossRels(
                        sort.getInput(), extraFieldBytes, detailIndex);
                yield rel.toBuilder().setSort(
                        sort.toBuilder().setInput(patched)).build();
            }
            case FETCH -> {
                var fetch = rel.getFetch();
                io.substrait.proto.Rel patched = patchCrossRels(
                        fetch.getInput(), extraFieldBytes, detailIndex);
                yield rel.toBuilder().setFetch(
                        fetch.toBuilder().setInput(patched)).build();
            }
            case AGGREGATE -> {
                var agg = rel.getAggregate();
                io.substrait.proto.Rel patched = patchCrossRels(
                        agg.getInput(), extraFieldBytes, detailIndex);
                yield rel.toBuilder().setAggregate(
                        agg.toBuilder().setInput(patched)).build();
            }
            case EXTENSION_SINGLE -> {
                var ext = rel.getExtensionSingle();
                io.substrait.proto.Rel patched = patchCrossRels(
                        ext.getInput(), extraFieldBytes, detailIndex);
                yield rel.toBuilder().setExtensionSingle(
                        ext.toBuilder().setInput(patched)).build();
            }
            default -> rel;
        };
    }

    private io.substrait.proto.Rel patchMultiInputNode(io.substrait.proto.Rel rel,
                                                         List<byte[]> extraFieldBytes,
                                                         int[] detailIndex) throws IOException {
        return switch (rel.getRelTypeCase()) {
            case EXTENSION_MULTI -> {
                var ext = rel.getExtensionMulti();
                var builder = ext.toBuilder();
                for (int i = 0; i < ext.getInputsCount(); i++) {
                    builder.setInputs(i, patchCrossRels(
                            ext.getInputs(i), extraFieldBytes, detailIndex));
                }
                yield rel.toBuilder().setExtensionMulti(builder).build();
            }
            case SET -> {
                var set = rel.getSet();
                var builder = set.toBuilder();
                for (int i = 0; i < set.getInputsCount(); i++) {
                    builder.setInputs(i, patchCrossRels(
                            set.getInputs(i), extraFieldBytes, detailIndex));
                }
                yield rel.toBuilder().setSet(builder).build();
            }
            default -> rel;
        };
    }

    /**
     * Appends extra field bytes to a CrossRel proto message.
     * Protobuf allows fields in any order, so appending is valid.
     */
    private io.substrait.proto.CrossRel appendFieldsToCrossRel(
            io.substrait.proto.CrossRel original, byte[] extraFields)
            throws InvalidProtocolBufferException {
        byte[] originalBytes = original.toByteArray();
        byte[] combined = new byte[originalBytes.length + extraFields.length];
        System.arraycopy(originalBytes, 0, combined, 0, originalBytes.length);
        System.arraycopy(extraFields, 0, combined, originalBytes.length, extraFields.length);
        return io.substrait.proto.CrossRel.parseFrom(combined);
    }

    /**
     * Maps substrait-java Join.JoinType to the proto CrossRel.JoinType enum value.
     */
    private static int mapJoinTypeToProto(Join.JoinType joinType) {
        return switch (joinType) {
            case INNER -> 1;
            case OUTER -> 2;
            case LEFT -> 3;
            case RIGHT -> 4;
            default -> 1;
        };
    }

    private io.substrait.proto.Plan parseProto(byte[] bytes) {
        try {
            return io.substrait.proto.Plan.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Invalid Protobuf bytes: " + e.getMessage(), e);
        }
    }

    private Plan convertFromProto(io.substrait.proto.Plan protoPlan) {
        try {
            var builder = ImmutablePlan.builder();
            for (io.substrait.proto.PlanRel planRel : protoPlan.getRelationsList()) {
                builder.addRoots(convertRoot(planRel));
            }
            return builder.build();
        } catch (Exception e) {
            throw new SubstraitConversionException(
                    PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE,
                    "Failed to reconstruct Plan from proto: " + e.getMessage(), e);
        }
    }

    private Plan.Root convertRoot(io.substrait.proto.PlanRel planRel) {
        io.substrait.proto.RelRoot relRoot = planRel.getRoot();
        Rel rel = new SubstraitPlanDeserializer().fromProto(relRoot.getInput());
        return ImmutableRoot.builder()
                .input(rel)
                .names(relRoot.getNamesList())
                .build();
    }
}
