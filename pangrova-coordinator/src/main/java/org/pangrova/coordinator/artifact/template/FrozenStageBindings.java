package org.pangrova.coordinator.artifact.template;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.pangrova.coordinator.artifact.PublishedArtifactBindingResolver;
import org.pangrova.metadata.artifact.build.ArtifactStageGraph.StagePlan;
import org.pangrova.metadata.artifact.build.template.ArtifactStageTemplate;
import org.pangrova.protocol.BoundExecutionPlan;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.OperatorBindingSet;
import org.pangrova.protocol.v2.ResultColumnLogicalType;
import org.pangrova.security.authz.DeferredAuthorizationScope.ModelSnapshot;
import org.pangrova.spi.annotation.Internal;

import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

/** Canonical immutable stage codec. Runtime attempt identities and provider credentials never enter its bytes. */
@Internal
public final class FrozenStageBindings {
    private FrozenStageBindings() {}

    /** Freezes the actual protected binding outcome, retaining policy, resource and exact logical type contracts. */
    public static ArtifactStageTemplate freeze(BoundExecutionPlan plan, List<ModelSnapshot> models) {
        var bindings = plan.operatorBindings().stream().map(FrozenStageBindings::withoutAttempt).toList();
        validate(bindings, plan.resultColumnTypes(), models);
        return ArtifactStageTemplate.of(OperatorBindingSet.newBuilder().addAllBindings(bindings).build().toByteArray(),
                plan.resultColumnTypes().stream().map(ResultColumnLogicalType::toByteArray).toList(),
                bindings.stream().flatMap(binding -> binding.getArtifactsList().stream())
                        .map(PublishedArtifactBindingResolver::publicationRead).distinct().toList());
    }

    /** Replays only graph-pinned bytes; model resolution and publication discovery are not replay operations. */
    public static BoundExecutionPlan read(StagePlan stage, List<ModelSnapshot> models) {
        var bindings = parse(stage.bindingSetBytes(), OperatorBindingSet.parser()).getBindingsList();
        var types = resultColumnTypes(stage);
        validate(bindings, types, models);
        var reads = bindings.stream().flatMap(binding -> binding.getArtifactsList().stream())
                .map(PublishedArtifactBindingResolver::publicationRead).collect(java.util.stream.Collectors.toSet());
        if (!reads.equals(Set.copyOf(stage.template().publicationReads()))) {
            throw new IllegalArgumentException("Frozen stage publication pointers differ from its binding bytes");
        }
        return new BoundExecutionPlan(stage.planBytes(), bindings, types);
    }

    /** Reads only the exact declared output contracts; this does not grant model or execution authority. */
    public static List<ResultColumnLogicalType> resultColumnTypes(StagePlan stage) {
        var types = stage.template().resultTypeBytes().stream()
                .map(bytes -> parse(bytes, ResultColumnLogicalType.parser())).toList();
        validateOutputTypes(types);
        if (types.stream().anyMatch(type -> type.getColumnIndex() >= stage.outputNames().size())) {
            throw new IllegalArgumentException("Frozen logical output ordinal exceeds its declared stage width");
        }
        return types;
    }

    private static OperatorBinding withoutAttempt(OperatorBinding binding) {
        var result = binding.toBuilder();
        if (binding.hasExecutionPolicy()) {
            result.setExecutionPolicy(binding.getExecutionPolicy().toBuilder()
                    .clearDeadlineEpochMs().clearIdempotencyKey());
        }
        if (binding.hasOutputArtifact()) {
            result.setOutputArtifact(binding.getOutputArtifact().toBuilder().clearExpectedSourceObjectIds()
                    .clearPublicationFencingToken().clearIdempotencyKey().clearWorkUnitId().clearWorkKind()
                    .clearOutputScopeId());
        }
        return result.build();
    }

    private static void validate(List<OperatorBinding> bindings, List<ResultColumnLogicalType> types,
            List<ModelSnapshot> models) {
        Set<String> nodes = new HashSet<>();
        for (var binding : bindings) {
            requireKnown(binding);
            if (binding.getPlanNodeId().isBlank() || !nodes.add(binding.getPlanNodeId()) || !binding.hasCapability()) {
                throw new IllegalArgumentException("Frozen stage operator identities must be exact and unique");
            }
            requireUnboundAttempt(binding);
            if (binding.hasModel()) requireModel(binding, models);
            binding.getArtifactsList().forEach(PublishedArtifactBindingResolver::publicationRead);
        }
        validateOutputTypes(types);
    }

    private static void validateOutputTypes(List<ResultColumnLogicalType> types) {
        Set<Integer> columns = new HashSet<>();
        for (var type : types) {
            requireKnown(type);
            requireLogicalType(type.getLogicalType());
            VectorLogicalType.requireStorageDimension(type.getLogicalType());
            if (type.getColumnIndex() < 0 || !columns.add(type.getColumnIndex())) {
                throw new IllegalArgumentException("Frozen stage logical output ordinals must be exact and unique");
            }
        }
    }

    private static void requireUnboundAttempt(OperatorBinding binding) {
        if (binding.getExecutionPolicy().getDeadlineEpochMs() != 0
                || !binding.getExecutionPolicy().getIdempotencyKey().isEmpty()) {
            throw new IllegalArgumentException("Frozen stage must not contain dispatched execution policy");
        }
        requireUnboundOutput(binding.getOutputArtifact());
    }

    private static void requireUnboundOutput(org.pangrova.protocol.v2.ArtifactWriteBinding output) {
        if (output.getPublicationFencingToken() != 0 || !output.getIdempotencyKey().isEmpty()
                || !output.getWorkUnitId().isEmpty() || output.getWorkKindValue() != 0
                || !output.getOutputScopeId().isEmpty() || output.getExpectedSourceObjectIdsCount() != 0) {
            throw new IllegalArgumentException(
                    "Frozen stage must not contain an artifact attempt or physical allocation");
        }
    }

    private static void requireModel(OperatorBinding binding, List<ModelSnapshot> models) {
        var model = binding.getModel();
        if (!model.getCredentialRef().isEmpty()) {
            throw new IllegalArgumentException("Frozen model binding must not retain provider credentials");
        }
        requireLogicalType(model.getOutputType());
        var ref = model.getModel();
        var matches = models.stream().filter(value -> value.logicalName().equals(ref.getId())).toList();
        if (matches.size() != 1 || !matches.get(0).deploymentVersion().equals(ref.getVersion())
                || !matches.get(0).signature().equals(
                        HexFormat.of().formatHex(ref.getSignatureSha256().toByteArray()))) {
            throw new IllegalArgumentException("Frozen model differs from the observed current model authority");
        }
    }

    private static void requireLogicalType(LogicalType type) {
        if (type.getCanonicalSql().isBlank() || !type.hasSubstraitType()
                || type.getSubstraitType().equals(type.getSubstraitType().getDefaultInstanceForType())) {
            throw new IllegalArgumentException("Frozen output requires its exact logical type contract");
        }
    }

    private static <T extends Message> T parse(byte[] bytes, Parser<T> parser) {
        try {
            T value = parser.parseFrom(bytes);
            requireKnown(value);
            if (!Arrays.equals(bytes, value.toByteArray())) {
                throw new IllegalArgumentException("Frozen stage protobuf bytes are not canonical");
            }
            return value;
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Frozen stage protobuf bytes are malformed", failure);
        }
    }

    private static void requireKnown(Message value) {
        if (!value.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Frozen stage contains unknown protocol fields");
        }
        for (var field : value.getAllFields().entrySet()) {
            if (field.getKey().getJavaType() == com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                if (field.getKey().isRepeated()) {
                    for (var nested : (List<?>) field.getValue()) requireKnown((Message) nested);
                } else {
                    requireKnown((Message) field.getValue());
                }
            }
        }
    }
}
