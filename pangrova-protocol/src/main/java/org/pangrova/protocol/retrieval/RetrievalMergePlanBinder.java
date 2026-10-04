package org.pangrova.protocol.retrieval;

import org.pangrova.protocol.BoundExecutionPlan;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.ExtensionSingleRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.Rel;
import org.pangrova.protocol.v2.CandidateContract;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.CoveragePolicy;
import org.pangrova.protocol.v2.ExchangeDistribution;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.FallbackPolicy;
import org.pangrova.protocol.v2.NamedExpressionArgument;
import org.pangrova.protocol.v2.NamedValue;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.RetrievalInputContract;
import org.pangrova.protocol.v2.RetrievalMergeDetail;
import org.pangrova.protocol.v2.RetryClass;
import org.pangrova.protocol.v2.ScoreDirection;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.spi.annotation.Internal;

/** Binds a typed retrieval merge directly above a complete singleton exchange. */
@Internal
public final class RetrievalMergePlanBinder {

    private static final int CANDIDATE_SCHEMA_VERSION = 1;

    private RetrievalMergePlanBinder() {}

    public static byte[] bind(
            byte[] planBytes, RetrievalMergeBinding binding, String planNodeId) {
        return bindExecution(planBytes, binding, planNodeId).planBytes();
    }

    /** Binds the merge relation and its task-level capability/policy contract. */
    public static BoundExecutionPlan bindExecution(
            byte[] planBytes, RetrievalMergeBinding binding, String planNodeId) {
        if (planNodeId == null || planNodeId.isBlank()) {
            throw new IllegalArgumentException("retrieval merge plan node ID is required");
        }
        try {
            Plan plan = Plan.parseFrom(planBytes);
            if (plan.getRelationsCount() != 1 || !plan.getRelations(0).hasRoot()) {
                throw new IllegalArgumentException(
                        "retrieval merge requires exactly one rooted fragment");
            }
            var root = plan.getRelations(0).getRoot();
            Rel input = root.getInput();
            RelationDetailEnvelope exchange = requireSingletonExchange(input);
            NamedStruct schema = NamedStruct.parseFrom(binding.candidateSchema());
            if (!schema.equals(exchange.getExchangeInput().getOutputSchema())) {
                throw new IllegalArgumentException(
                        "retrieval merge candidate schema differs from exchange schema");
            }
            validateIndexes(binding, schema.getStruct().getTypesCount());

            RetrievalMergeDetail detail = buildDetail(binding, schema, planNodeId);
            Rel merged = Rel.newBuilder().setExtensionSingle(
                    ExtensionSingleRel.newBuilder()
                            .setInput(input)
                            .setDetail(Any.pack(RelationDetailEnvelope.newBuilder()
                                    .setSchemaVersion(1)
                                    .setRetrievalMerge(detail)
                                    .build())))
                    .build();
            Plan.Builder output = plan.toBuilder();
            output.setRelations(0, plan.getRelations(0).toBuilder().setRoot(
                    root.toBuilder().setInput(merged)));
            return new BoundExecutionPlan(
                    output.build().toByteArray(),
                    java.util.List.of(operatorBinding(detail, binding)));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(
                    "invalid Substrait plan or retrieval candidate schema", e);
        }
    }

    /**
     * Replaces the output stage's original global Top-K with a Native merge over
     * source-tagged rows from a complete singleton exchange. The internal source
     * tag is removed before the user-visible root.
     */
    public static BoundExecutionPlan bindFinalTopKExecution(
            byte[] planBytes, RetrievalMergeBinding binding, String planNodeId) {
        return bindFinalTopKExecution(
                planBytes, binding, binding.outputLimit(), planNodeId);
    }

    /**
     * Replaces an original final Top-K whose visible limit may be smaller than
     * the merge output when a bounded rerank stage follows it.
     */
    public static BoundExecutionPlan bindFinalTopKExecution(
            byte[] planBytes,
            RetrievalMergeBinding binding,
            int expectedFinalLimit,
            String planNodeId) {
        if (expectedFinalLimit <= 0 || expectedFinalLimit > binding.outputLimit()) {
            throw new IllegalArgumentException(
                    "retrieval merge final limit is invalid");
        }
        if (planNodeId == null || planNodeId.isBlank()) {
            throw new IllegalArgumentException("retrieval merge plan node ID is required");
        }
        try {
            Plan plan = Plan.parseFrom(planBytes);
            return bindFinalTopK(plan, binding, expectedFinalLimit, planNodeId);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException(
                    "invalid Substrait output-stage merge plan", failure);
        }
    }

    private static BoundExecutionPlan bindFinalTopK(
            Plan plan,
            RetrievalMergeBinding binding,
            int expectedFinalLimit,
            String planNodeId) throws InvalidProtocolBufferException {
        requireRootedPlan(plan);
        var root = plan.getRelations(0).getRoot();
        Rel exchange = requireFinalTopK(root.getInput(), expectedFinalLimit);
        RelationDetailEnvelope exchangeEnvelope = requireSingletonExchange(exchange);
        NamedStruct candidateSchema = NamedStruct.parseFrom(binding.candidateSchema());
        validateFinalSchema(binding, candidateSchema, exchangeEnvelope);
        Rel augmentedExchange = augmentExchange(exchange, exchangeEnvelope, candidateSchema);
        RetrievalMergeDetail detail = buildDetail(binding, candidateSchema, planNodeId);
        Rel merged = mergeRelation(augmentedExchange, detail);
        Rel visible = projectVisibleFields(merged, candidateSchema);
        Plan output = plan.toBuilder().setRelations(0,
                plan.getRelations(0).toBuilder().setRoot(
                        root.toBuilder().setInput(visible))).build();
        return new BoundExecutionPlan(
                output.toByteArray(), java.util.List.of(operatorBinding(detail, binding)));
    }

    private static void requireRootedPlan(Plan plan) {
        if (plan.getRelationsCount() != 1 || !plan.getRelations(0).hasRoot()) {
            throw new IllegalArgumentException(
                    "retrieval merge requires exactly one rooted fragment");
        }
    }

    private static Rel requireFinalTopK(Rel input, int expectedFinalLimit) {
        if (!input.hasFetch() || SubstraitFetchBounds.offset(input.getFetch()) != 0
                || SubstraitFetchBounds.count(input.getFetch()) != expectedFinalLimit
                || !input.getFetch().getInput().hasSort()) {
            throw new IllegalArgumentException(
                    "retrieval merge output stage requires a matching global Top-K");
        }
        return input.getFetch().getInput().getSort().getInput();
    }

    private static void validateFinalSchema(
            RetrievalMergeBinding binding,
            NamedStruct candidateSchema,
            RelationDetailEnvelope exchangeEnvelope) {
        if (binding.sourceIdFieldIndex()
                != candidateSchema.getStruct().getTypesCount() - 1) {
            throw new IllegalArgumentException(
                    "retrieval merge source tag must be the final internal field");
        }
        NamedStruct visibleSchema = removeFinalField(candidateSchema);
        if (!visibleSchema.equals(exchangeEnvelope.getExchangeInput().getOutputSchema())) {
            throw new IllegalArgumentException(
                    "retrieval merge visible schema differs from exchange schema");
        }
        validateIndexes(binding, candidateSchema.getStruct().getTypesCount());
    }

    private static Rel augmentExchange(
            Rel exchange,
            RelationDetailEnvelope envelope,
            NamedStruct candidateSchema) {
        RelationDetailEnvelope rewritten = envelope.toBuilder()
                .setExchangeInput(envelope.getExchangeInput().toBuilder()
                        .setOutputSchema(candidateSchema))
                .build();
        return exchange.toBuilder().setExtensionLeaf(
                exchange.getExtensionLeaf().toBuilder().setDetail(Any.pack(rewritten))).build();
    }

    private static Rel mergeRelation(Rel input, RetrievalMergeDetail detail) {
        return Rel.newBuilder().setExtensionSingle(
                ExtensionSingleRel.newBuilder()
                        .setInput(input)
                        .setDetail(Any.pack(RelationDetailEnvelope.newBuilder()
                                .setSchemaVersion(1)
                                .setRetrievalMerge(detail)
                                .build())))
                .build();
    }

    private static Rel projectVisibleFields(Rel merged, NamedStruct candidateSchema) {
        ProjectRel.Builder visible = ProjectRel.newBuilder().setInput(merged);
        int fields = candidateSchema.getStruct().getTypesCount();
        for (int index = 0;
                index < fields - 1;
                ++index) {
            visible.addExpressions(fieldReference(index));
            visible.getCommonBuilder().getEmitBuilder().addOutputMapping(fields + index);
        }
        return Rel.newBuilder().setProject(visible).build();
    }

    private static NamedStruct removeFinalField(NamedStruct schema) {
        int fieldCount = schema.getStruct().getTypesCount();
        return SubstraitRelationLayout.projectSchema(schema,
                java.util.stream.IntStream.range(0, fieldCount - 1).boxed().toList());
    }

    private static OperatorBinding operatorBinding(
            RetrievalMergeDetail detail, RetrievalMergeBinding binding) {
        long candidateLimit = binding.sources().stream()
                .mapToLong(RetrievalMergeBinding.Source::candidateLimit)
                .sum();
        return OperatorBinding.newBuilder()
                .setPlanNodeId(detail.getPlanNodeId())
                .setCapability(detail.getInvocation().getCapability())
                .setResources(org.pangrova.protocol.v2
                        .OperatorResourceRequirement.newBuilder()
                        .setCandidateRows(candidateLimit))
                .setExecutionPolicy(ExecutionPolicy.newBuilder()
                        .setCoveragePolicy(
                                CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE)
                        .setFallbackPolicy(FallbackPolicy.FALLBACK_POLICY_FORBID)
                        .setRetryClass(RetryClass.RETRY_CLASS_IDEMPOTENT)
                        .setApproximation(org.pangrova.protocol.v2.ApproximationProfile
                                .newBuilder()
                                .setProfileId("bounded-fusion")
                                .setCandidateLimit(candidateLimit)
                                .setExactRescoreLimit(binding.outputLimit())
                                .setRequiredRecallAtK(1.0)
                                .setRecallK(binding.outputLimit())))
                .build();
    }

    private static RelationDetailEnvelope requireSingletonExchange(Rel input)
            throws InvalidProtocolBufferException {
        if (!input.hasExtensionLeaf()) {
            throw new IllegalArgumentException(
                    "retrieval merge must be placed directly above an exchange input");
        }
        RelationDetailEnvelope envelope = input.getExtensionLeaf().getDetail().unpack(
                RelationDetailEnvelope.class);
        if (envelope.getSchemaVersion() != 1 || !envelope.hasExchangeInput()
                || envelope.getExchangeInput().getDistribution()
                        != ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON
                || envelope.getExchangeInput().getPartitionCount() != 1
                || !envelope.getExchangeInput().getRequireComplete()) {
            throw new IllegalArgumentException(
                    "retrieval merge requires a complete singleton exchange input");
        }
        return envelope;
    }

    private static void validateIndexes(RetrievalMergeBinding binding, int fieldCount) {
        if (binding.scoreFieldIndex() >= fieldCount
                || binding.sourceIdFieldIndex() >= fieldCount
                || binding.stableKeyFieldIndexes().stream()
                        .anyMatch(index -> index >= fieldCount)) {
            throw new IllegalArgumentException(
                    "retrieval merge field index is outside the candidate schema");
        }
    }

    private static RetrievalMergeDetail buildDetail(
            RetrievalMergeBinding binding, NamedStruct schema, String planNodeId) {
        CandidateContract outputContract = contract(
                schema,
                ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER,
                binding);
        CapabilityInvocation.Builder invocation = CapabilityInvocation.newBuilder()
                .setCapability(binding.strategy() == RetrievalMergeBinding.Strategy.RRF
                        ? BuiltInCapabilityCatalog.rrfMergePlanRef()
                        : BuiltInCapabilityCatalog.linearMergePlanRef())
                .addArguments(argument("score", binding.scoreFieldIndex()))
                .addArguments(argument("source_id", binding.sourceIdFieldIndex()));
        if (binding.strategy() == RetrievalMergeBinding.Strategy.RRF) {
            invocation.addOptions(NamedValue.newBuilder()
                    .setName("rrf_k")
                    .setValue(TypedValue.newBuilder().setInt64Value(binding.rrfK())));
        } else {
            for (RetrievalMergeBinding.Source source : binding.sources()) {
                invocation.addOptions(NamedValue.newBuilder()
                        .setName("weight." + source.sourceId())
                        .setValue(TypedValue.newBuilder().setDoubleValue(source.weight())));
            }
        }

        RetrievalMergeDetail.Builder detail = RetrievalMergeDetail.newBuilder()
                .setPlanNodeId(planNodeId)
                .setInvocation(invocation)
                .setOutputLimit(binding.outputLimit())
                .setMaxFanIn(binding.maxFanIn())
                .setOutputContract(outputContract);
        for (RetrievalMergeBinding.Source source : binding.sources()) {
            detail.addInputs(RetrievalInputContract.newBuilder()
                    .setSourceId(source.sourceId())
                    .setContract(contract(schema, source.scoreDirection(), binding)));
        }
        return detail.build();
    }

    private static CandidateContract contract(
            NamedStruct schema,
            ScoreDirection direction,
            RetrievalMergeBinding binding) {
        CandidateContract.Builder contract = CandidateContract.newBuilder()
                .setSchemaVersion(CANDIDATE_SCHEMA_VERSION)
                .setSchema(schema)
                .setScoreDirection(direction);
        for (int index : binding.stableKeyFieldIndexes()) {
            contract.addStableKeyFields(SubstraitRelationLayout.topLevelNames(schema).get(index));
        }
        return contract.build();
    }

    private static NamedExpressionArgument argument(String name, int index) {
        return NamedExpressionArgument.newBuilder()
                .setName(name)
                .setExpression(SubstraitRelationLayout.rootFieldReference(index))
                .build();
    }

    private static io.substrait.proto.Expression fieldReference(int index) {
        return SubstraitRelationLayout.rootFieldReference(index);
    }
}
