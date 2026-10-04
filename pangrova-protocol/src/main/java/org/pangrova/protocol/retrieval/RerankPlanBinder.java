package org.pangrova.protocol.retrieval;

import org.pangrova.protocol.BoundExecutionPlan;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.substrait.proto.Expression;
import io.substrait.proto.ExtensionSingleRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import org.pangrova.protocol.v2.CandidateContract;
import org.pangrova.protocol.v2.CapabilityInvocation;
import org.pangrova.protocol.v2.CoveragePolicy;
import org.pangrova.protocol.v2.ExecutionPolicy;
import org.pangrova.protocol.v2.FallbackPolicy;
import org.pangrova.protocol.v2.ModelBinding;
import org.pangrova.protocol.v2.ModelRef;
import org.pangrova.protocol.v2.NamedExpressionArgument;
import org.pangrova.protocol.v2.OperatorBinding;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.RerankDetail;
import org.pangrova.protocol.v2.RetryClass;
import org.pangrova.protocol.v2.ScoreDirection;
import org.pangrova.protocol.v2.VersionedRef;
import org.pangrova.spi.annotation.Internal;

/** Binds a complete bounded candidate stream to a governed Native rerank relation. */
@Internal
public final class RerankPlanBinder {

    private RerankPlanBinder() {}

    /**
     * Rewrites the output stage's original global Top-K into a bounded
     * pre-rerank candidate pool and applies the final governed rerank above it.
     */
    public static BoundExecutionPlan bindFinalTopKExecution(
            byte[] planBytes, RerankBinding binding, String planNodeId) {
        try {
            Plan plan = Plan.parseFrom(planBytes);
            if (plan.getRelationsCount() != 1 || !plan.getRelations(0).hasRoot()) {
                throw new IllegalArgumentException(
                        "rerank requires exactly one rooted fragment");
            }
            var root = plan.getRelations(0).getRoot();
            if (!root.getInput().hasFetch()
                    || SubstraitFetchBounds.offset(root.getInput().getFetch()) != 0
                    || SubstraitFetchBounds.count(root.getInput().getFetch())
                            != binding.outputLimit()
                    || !root.getInput().getFetch().getInput().hasSort()) {
                throw new IllegalArgumentException(
                        "rerank output stage requires a matching global Top-K");
            }
            Rel candidates = root.getInput().toBuilder().setFetch(
                    root.getInput().getFetch().toBuilder()
                            .setCountExpr(SubstraitFetchBounds.literal(binding.inputLimit()))).build();
            Plan overfetch = plan.toBuilder().setRelations(
                    0, plan.getRelations(0).toBuilder().setRoot(
                            root.toBuilder().setInput(candidates))).build();
            return bindExecution(overfetch.toByteArray(), binding, planNodeId);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException(
                    "invalid Substrait output-stage rerank plan", failure);
        }
    }

    /**
     * Binds one governed rerank relation above a complete candidate stream.
     *
     * @param planBytes serialized Substrait plan
     * @param binding validated rerank binding
     * @param planNodeId canonical relation identifier
     * @return rewritten plan and task-level operator binding
     */
    public static BoundExecutionPlan bindExecution(
            byte[] planBytes, RerankBinding binding, String planNodeId) {
        if (planNodeId == null || planNodeId.isBlank()) {
            throw new IllegalArgumentException("rerank plan node ID is required");
        }
        try {
            Plan plan = Plan.parseFrom(planBytes);
            return bindParsed(plan, binding, planNodeId);
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException(
                    "invalid Substrait plan or rerank candidate schema", failure);
        }
    }

    private static BoundExecutionPlan bindParsed(
            Plan plan, RerankBinding binding, String planNodeId)
            throws InvalidProtocolBufferException {
        if (plan.getRelationsCount() != 1 || !plan.getRelations(0).hasRoot()) {
            throw new IllegalArgumentException("rerank requires exactly one rooted fragment");
        }
        var root = plan.getRelations(0).getRoot();
        NamedStruct schema = NamedStruct.parseFrom(binding.candidateSchema());
        validateSchema(binding, schema);
        RerankDetail detail = buildDetail(binding, schema, planNodeId);
        Rel rerank = Rel.newBuilder().setExtensionSingle(
                ExtensionSingleRel.newBuilder()
                        .setInput(root.getInput())
                        .setDetail(Any.pack(RelationDetailEnvelope.newBuilder()
                                .setSchemaVersion(1)
                                .setRerank(detail)
                                .build())))
                .build();
        Plan bound = plan.toBuilder().setRelations(0,
                plan.getRelations(0).toBuilder().setRoot(
                        root.toBuilder().setInput(rerank))).build();
        return new BoundExecutionPlan(
                bound.toByteArray(), java.util.List.of(operatorBinding(detail, binding)));
    }

    private static RerankDetail buildDetail(
            RerankBinding binding, NamedStruct schema, String planNodeId) {
        ModelRef model = ModelRef.newBuilder()
                .setModelId(binding.modelId())
                .setVersion(binding.modelVersion())
                .setSignatureSha256(ByteString.copyFrom(binding.modelSignature()))
                .build();
        CandidateContract.Builder output = CandidateContract.newBuilder()
                .setSchemaVersion(1)
                .setSchema(schema)
                .setScoreDirection(ScoreDirection.SCORE_DIRECTION_HIGHER_IS_BETTER);
        binding.stableKeyFieldIndexes().forEach(index ->
                output.addStableKeyFields(schema.getNames(index)));
        CapabilityInvocation invocation = CapabilityInvocation.newBuilder()
                .setCapability(BuiltInCapabilityCatalog.crossEncoderRerankPlanRef())
                .addArguments(argument("score", binding.scoreFieldIndex()))
                .addArguments(literal("query", binding.query()))
                .addArguments(argument("document", binding.documentFieldIndex()))
                .build();
        return RerankDetail.newBuilder()
                .setPlanNodeId(planNodeId)
                .setInvocation(invocation)
                .setInputLimit(binding.inputLimit())
                .setOutputLimit(binding.outputLimit())
                .setOutputContract(output)
                .setModel(model)
                .build();
    }

    private static void validateSchema(
            RerankBinding binding, NamedStruct schema) {
        int fields = schema.getStruct().getTypesCount();
        if (schema.getNamesCount() != fields
                || binding.scoreFieldIndex() >= fields
                || binding.documentFieldIndex() >= fields
                || binding.stableKeyFieldIndexes().stream()
                        .anyMatch(index -> index >= fields)
                || !schema.getStruct().getTypes(binding.scoreFieldIndex())
                        .hasFp64()
                || !schema.getStruct().getTypes(binding.documentFieldIndex())
                        .hasString()) {
            throw new IllegalArgumentException(
                    "rerank candidate schema does not match field bindings");
        }
    }

    private static OperatorBinding operatorBinding(
            RerankDetail detail, RerankBinding binding) {
        VersionedRef modelRef = VersionedRef.newBuilder()
                .setId(binding.modelId())
                .setVersion(binding.modelVersion())
                .setSignatureSha256(ByteString.copyFrom(
                        binding.modelSignature()))
                .build();
        return OperatorBinding.newBuilder()
                .setPlanNodeId(detail.getPlanNodeId())
                .setCapability(detail.getInvocation().getCapability())
                .setModel(ModelBinding.newBuilder()
                        .setModel(modelRef)
                        .setOutputType(binding.modelOutputType())
                        .setRequestLimits(org.pangrova.protocol.ModelResourceEnvelopes
                                .requestLimits(binding.modelLimits())))
                .setResources(org.pangrova.protocol.ModelResourceEnvelopes.resources(
                        binding.modelLimits(), binding.inputLimit(), false).toBuilder()
                        .setCandidateRows(binding.inputLimit()))
                .setExecutionPolicy(ExecutionPolicy.newBuilder()
                        .setCoveragePolicy(
                                CoveragePolicy.COVERAGE_POLICY_REQUIRE_COMPLETE)
                        .setFallbackPolicy(FallbackPolicy.FALLBACK_POLICY_FORBID)
                        .setRetryClass(
                                RetryClass.RETRY_CLASS_PROVIDER_IDEMPOTENCY_KEY)
                        .setApproximation(org.pangrova.protocol.v2
                                .ApproximationProfile.newBuilder()
                                .setProfileId("bounded-cross-encoder")
                                .setCandidateLimit(binding.inputLimit())
                                .setExactRescoreLimit(binding.inputLimit())
                                .setRequiredRecallAtK(1.0)
                                .setRecallK(binding.outputLimit())))
                .build();
    }

    private static NamedExpressionArgument argument(String name, int index) {
        return NamedExpressionArgument.newBuilder()
                .setName(name)
                .setExpression(field(index))
                .build();
    }

    private static NamedExpressionArgument literal(String name, String value) {
        return NamedExpressionArgument.newBuilder()
                .setName(name)
                .setExpression(Expression.newBuilder().setLiteral(
                        Expression.Literal.newBuilder().setString(value)))
                .build();
    }

    private static Expression field(int index) {
        return SubstraitRelationLayout.rootFieldReference(index);
    }
}
