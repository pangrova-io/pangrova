package org.pangrova.protocol.retrieval;

import org.pangrova.protocol.execution.BoundExecutionPlan;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.substrait.proto.Expression;
import io.substrait.proto.ExtensionSingleRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Rel;
import io.substrait.proto.SortField;
import org.pangrova.protocol.v2.CandidateContract;
import org.pangrova.protocol.substrait.SubstraitRelationLayout;
import org.pangrova.protocol.v2.NamedExpressionArgument;
import org.pangrova.protocol.v2.RelationDetailEnvelope;
import org.pangrova.protocol.v2.RetrievalSourceDetail;
import org.pangrova.common.api.annotation.Internal;

/** Binds wire-neutral planner Top-K intents into canonical v2 Substrait relation details. */
@Internal
public final class RetrievalTopKPlanBinder {

    private static final int CANDIDATE_SCHEMA_VERSION = RetrievalCandidateIdentity.SCHEMA_VERSION;
    private static final String REL_MESSAGE_NAME = "substrait.Rel";

    private RetrievalTopKPlanBinder() {}

    /** Replaces each matching Sort/Fetch pair and fails if any intent is unmatched. */
    public static byte[] bind(
            byte[] planBytes,
            List<RetrievalTopKBinding> bindings,
            String idPrefix) {
        return bindExecution(planBytes, bindings, idPrefix).planBytes();
    }

    /** Binds the plan and returns the matching task-level execution contracts. */
    public static BoundExecutionPlan bindExecution(
            byte[] planBytes,
            List<RetrievalTopKBinding> bindings,
            String idPrefix) {
        if (bindings.isEmpty()) {
            return new BoundExecutionPlan(planBytes, List.of());
        }
        try {
            io.substrait.proto.Plan plan = io.substrait.proto.Plan.parseFrom(planBytes);
            RetrievalTopKBindingCursor cursor = new RetrievalTopKBindingCursor(
                    bindings, idPrefix, SubstraitFunctionNames.index(plan));
            io.substrait.proto.Plan.Builder builder = plan.toBuilder();
            for (int i = 0; i < builder.getRelationsCount(); i++) {
                var planRel = builder.getRelations(i);
                if (planRel.hasRoot()) {
                    builder.setRelations(i, planRel.toBuilder().setRoot(
                            planRel.getRoot().toBuilder().setInput(
                                    rewriteRel(planRel.getRoot().getInput(), cursor))));
                }
            }
            if (cursor.boundCount() != bindings.size()) {
                throw new IllegalArgumentException(
                        "Unmatched exact Top-K bindings: expected=" + bindings.size()
                                + " bound=" + cursor.boundCount());
            }
            return new BoundExecutionPlan(
                    builder.build().toByteArray(), cursor.operatorBindings());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Invalid Substrait plan or candidate schema", e);
        }
    }

    private static Rel rewriteRel(Rel rel, RetrievalTopKBindingCursor cursor)
            throws InvalidProtocolBufferException {
        FieldDescriptor active = rel.getDescriptorForType().findFieldByNumber(
                rel.getRelTypeCase().getNumber());
        if (active == null || !rel.hasField(active)) {
            return rel;
        }
        Message payload = (Message) rel.getField(active);
        Message rewrittenPayload = rewriteInputs(payload, cursor);
        Rel rewritten = rel.toBuilder().setField(active, rewrittenPayload).build();
        return rewritten.hasFetch() ? tryBindFetch(rewritten, cursor) : rewritten;
    }

    private static Message rewriteInputs(
            Message payload, RetrievalTopKBindingCursor cursor)
            throws InvalidProtocolBufferException {
        Message.Builder builder = payload.toBuilder();
        for (FieldDescriptor field : payload.getDescriptorForType().getFields()) {
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                    || !field.getMessageType().getFullName().equals(REL_MESSAGE_NAME)) {
                continue;
            }
            if (field.isRepeated()) {
                for (int i = 0; i < payload.getRepeatedFieldCount(field); i++) {
                    builder.setRepeatedField(field, i, rewriteRel(
                            (Rel) payload.getRepeatedField(field, i), cursor));
                }
            } else if (payload.hasField(field)) {
                builder.setField(field, rewriteRel((Rel) payload.getField(field), cursor));
            }
        }
        return builder.build();
    }

    private static Rel tryBindFetch(Rel rel, RetrievalTopKBindingCursor cursor)
            throws InvalidProtocolBufferException {
        if (!isFetchCandidate(rel, cursor)) {
            return rel;
        }
        var fetch = rel.getFetch();
        var sort = fetch.getInput().getSort();
        RetrievalTopKBinding binding = cursor.peek();
        if (!matchesBinding(fetch, sort, binding)) {
            return rel;
        }

        Rel retrievalInput = sort.getInput();
        NamedStruct boundSchema = candidateSchema(retrievalInput, binding);
        List<String> names = SubstraitRelationLayout.topLevelNames(boundSchema);
        List<String> requestedNames = SubstraitRelationLayout.topLevelNames(
                NamedStruct.parseFrom(binding.candidateSchema()));
        boolean ambiguousIdentity = hasAmbiguousIdentity(names, binding);
        if (ambiguousIdentity && !binding.exact()) {
            throw new IllegalArgumentException("Artifact-backed Top-K requires distinct candidate identity fields");
        }
        if (binding.exact() && (ambiguousIdentity || !names.equals(requestedNames))) {
            // The ordinary Sort/Fetch handles SQL ORDER BY without inventing an
            // identity from two columns that resolve to the same field name.
            cursor.retainExactSortFetch();
            return rel;
        }
        if (binding.capability().getCapabilityId().equals(
                BuiltInCapabilityCatalog.BM25_RETRIEVAL_ID)) {
            retrievalInput = Bm25MaterializedScoreBinder.bind(
                    retrievalInput, binding, cursor.functionNames());
        }
        RetrievalSourceDetail detail = buildDetail(binding, boundSchema, names, cursor);
        ExtensionSingleRel.Builder extension = ExtensionSingleRel.newBuilder()
                .setInput(retrievalInput)
                .setDetail(Any.pack(RelationDetailEnvelope.newBuilder()
                        .setSchemaVersion(1)
                        .setRetrievalSource(detail)
                        .build()));
        extension.setCommon(SubstraitRelationLayout.compose(sort.getCommon(), fetch.getCommon(), names.size()));
        cursor.advance(detail);
        return Rel.newBuilder().setExtensionSingle(extension).build();
    }

    private static boolean hasAmbiguousIdentity(
            List<String> names, RetrievalTopKBinding binding) {
        Set<String> used = new HashSet<>();
        used.add(names.get(binding.scoreFieldIndex()));
        for (int stableKey : binding.stableKeyFieldIndexes()) {
            if (stableKey < 0 || stableKey >= names.size() || !used.add(names.get(stableKey))) {
                return true;
            }
        }
        return false;
    }

    private static NamedStruct candidateSchema(Rel input, RetrievalTopKBinding binding)
            throws InvalidProtocolBufferException {
        NamedStruct actual = SubstraitRelationLayout.describe(input).outputSchema();
        NamedStruct requested = NamedStruct.parseFrom(binding.candidateSchema());
        if (!requested.hasStruct() || !actual.getStruct().getTypesList().equals(requested.getStruct().getTypesList())) {
            throw new IllegalArgumentException("Candidate schema does not match Top-K input types");
        }
        return actual;
    }

    private static boolean isFetchCandidate(
            Rel rel, RetrievalTopKBindingCursor cursor) {
        return cursor.hasNext() && SubstraitFetchBounds.offset(rel.getFetch()) == 0
                && rel.getFetch().getInput().hasSort();
    }

    private static boolean matchesBinding(
            io.substrait.proto.FetchRel fetch,
            io.substrait.proto.SortRel sort,
            RetrievalTopKBinding binding) {
        return SubstraitFetchBounds.count(fetch) == binding.outputLimit() && sort.getSortsCount() == 1
                && scoreFieldIndex(sort.getSorts(0)) == binding.scoreFieldIndex()
                && directionMatches(sort.getSorts(0).getDirection(), binding);
    }

    private static RetrievalSourceDetail buildDetail(
            RetrievalTopKBinding binding,
            NamedStruct schema,
            List<String> names,
            RetrievalTopKBindingCursor cursor) {
        Expression score = SubstraitFieldReference.fieldReference(binding.scoreFieldIndex());
        CandidateContract.Builder contract = CandidateContract.newBuilder()
                .setSchemaVersion(CANDIDATE_SCHEMA_VERSION)
                .setIdentityKind(identityKind(binding))
                .setSchema(schema)
                .setScoreDirection(binding.scoreDirection());
        for (int stableKey : binding.stableKeyFieldIndexes()) {
            if (stableKey < 0 || stableKey >= names.size()
                    || stableKey == binding.scoreFieldIndex()) {
                throw new IllegalArgumentException("Invalid Top-K stable key index: " + stableKey);
            }
            contract.addStableKeyFields(names.get(stableKey));
        }
        String nodeId = cursor.nextNodeId();
        var invocation = org.pangrova.protocol.v2.CapabilityInvocation.newBuilder()
                .setCapability(binding.capability())
                .addArguments(NamedExpressionArgument.newBuilder()
                        .setName("score")
                        .setExpression(score))
                .addAllOptions(binding.invocationOptions());
        return RetrievalSourceDetail.newBuilder()
                .setPlanNodeId(nodeId)
                .setSourceId(binding.exact() ? nodeId + "-source" : binding.sourceId())
                .setCandidateLimit(binding.candidateLimit())
                .setOutputLimit(binding.outputLimit())
                .setExactRescoreLimit(binding.exactRescoreLimit())
                .setInvocation(invocation)
                .setOutputContract(contract)
                .build();
    }

    private static org.pangrova.protocol.v2.CandidateIdentityKind identityKind(RetrievalTopKBinding binding) {
        try {
            return RetrievalCandidateIdentity.kind(NamedStruct.parseFrom(binding.candidateSchema()),
                    binding.stableKeyFieldIndexes());
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid retrieval identity schema", failure);
        }
    }

    private static int scoreFieldIndex(SortField sort) {
        if (!sort.hasExpr() || !sort.getExpr().hasSelection()
                || !sort.getExpr().getSelection().hasDirectReference()
                || !sort.getExpr().getSelection().getDirectReference().hasStructField()) {
            return -1;
        }
        return sort.getExpr().getSelection().getDirectReference().getStructField().getField();
    }

    private static boolean directionMatches(
            SortField.SortDirection direction, RetrievalTopKBinding binding) {
        return switch (binding.scoreDirection()) {
            case SCORE_DIRECTION_LOWER_IS_BETTER ->
                    direction == SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_FIRST
                            || direction == SortField.SortDirection.SORT_DIRECTION_ASC_NULLS_LAST;
            case SCORE_DIRECTION_HIGHER_IS_BETTER ->
                    direction == SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_FIRST
                            || direction == SortField.SortDirection.SORT_DIRECTION_DESC_NULLS_LAST;
            default -> false;
        };
    }

}
