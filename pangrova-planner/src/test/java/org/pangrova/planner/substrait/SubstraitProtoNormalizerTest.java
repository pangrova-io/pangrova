package org.pangrova.planner.substrait;

import com.google.protobuf.Message;
import io.substrait.proto.ComparisonJoinKey;
import io.substrait.proto.Expression;
import io.substrait.proto.FetchRel;
import io.substrait.proto.HashJoinRel;
import io.substrait.proto.MergeJoinRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ProjectRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests canonical Join keys at the SDK-to-wire boundary, including malformed SDK output. */
class SubstraitProtoNormalizerTest {

    @ParameterizedTest
    @ValueSource(strings = {"hash_join", "merge_join"})
    void removesVerifiedRedundantKeysInsideNestedPlan(String relationField) {
        Message.Builder join = newJoin(relationField);
        addCanonicalKey(join, equalKey());
        addDeprecatedKey(join, "left_keys", field(0));
        addDeprecatedKey(join, "right_keys", field(1));
        Plan input = nestedPlan(join.build(), relationField);

        Plan normalized = SubstraitProtoNormalizer.normalize(input);
        Message actualJoin = getNestedJoin(normalized, relationField);

        assertThat(actualJoin.getRepeatedFieldCount(descriptor(actualJoin, "left_keys"))).isZero();
        assertThat(actualJoin.getRepeatedFieldCount(descriptor(actualJoin, "right_keys"))).isZero();
        assertThat(actualJoin.getRepeatedField(descriptor(actualJoin, "keys"), 0)).isEqualTo(equalKey());
        assertThat(getNestedJoin(input, relationField)
                .getRepeatedFieldCount(descriptor(actualJoin, "left_keys"))).isOne();
        assertThat(SubstraitProtoNormalizer.normalize(normalized)).isSameAs(normalized);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hash_join", "merge_join"})
    void preservesCanonicalComparisonSemanticsWithoutDeprecatedKeys(String relationField) {
        Message.Builder join = newJoin(relationField);
        for (var comparison : new ComparisonJoinKey.SimpleComparisonType[] {
                ComparisonJoinKey.SimpleComparisonType.SIMPLE_COMPARISON_TYPE_EQ,
                ComparisonJoinKey.SimpleComparisonType.SIMPLE_COMPARISON_TYPE_IS_NOT_DISTINCT_FROM,
                ComparisonJoinKey.SimpleComparisonType.SIMPLE_COMPARISON_TYPE_MIGHT_EQUAL}) {
            addCanonicalKey(join, equalKey().toBuilder().setComparison(
                    ComparisonJoinKey.ComparisonType.newBuilder().setSimple(comparison)).build());
        }
        addCanonicalKey(join, equalKey().toBuilder().setComparison(
                ComparisonJoinKey.ComparisonType.newBuilder().setCustomFunctionReference(17)).build());
        Plan input = nestedPlan(join.build(), relationField);

        assertThat(SubstraitProtoNormalizer.normalize(input)).isSameAs(input);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hash_join", "merge_join"})
    void rejectsDeprecatedOnlyKeysRatherThanTranslatingLegacyPlans(String relationField) {
        Message.Builder join = newJoin(relationField);
        addDeprecatedKey(join, "left_keys", field(0));
        addDeprecatedKey(join, "right_keys", field(1));

        assertRejected(join, relationField, "Deprecated join keys differ from canonical keys");
    }

    @ParameterizedTest
    @ValueSource(strings = {"hash_join", "merge_join"})
    void rejectsOneSidedDeprecatedKeys(String relationField) {
        Message.Builder join = newJoin(relationField);
        addCanonicalKey(join, equalKey());
        addDeprecatedKey(join, "left_keys", field(0));

        assertRejected(join, relationField, "Deprecated join keys differ from canonical keys");
    }

    @ParameterizedTest
    @ValueSource(strings = {"hash_join", "merge_join"})
    void rejectsDeprecatedKeyCountMismatch(String relationField) {
        Message.Builder join = newJoin(relationField);
        addCanonicalKey(join, equalKey());
        addCanonicalKey(join, equalKey());
        addDeprecatedKey(join, "left_keys", field(0));
        addDeprecatedKey(join, "right_keys", field(1));

        assertRejected(join, relationField, "Deprecated join keys differ from canonical keys");
    }

    @ParameterizedTest
    @ValueSource(strings = {"hash_join", "merge_join"})
    void rejectsConflictingDeprecatedKeys(String relationField) {
        Message.Builder join = newJoin(relationField);
        addCanonicalKey(join, equalKey());
        addDeprecatedKey(join, "left_keys", field(2));
        addDeprecatedKey(join, "right_keys", field(1));

        assertRejected(join, relationField, "Deprecated join keys differ from canonical keys");
    }

    @ParameterizedTest
    @ValueSource(strings = {"hash_join", "merge_join"})
    void rejectsNonEqualComparisonWithLossyDeprecatedCopies(String relationField) {
        Message.Builder join = newJoin(relationField);
        addCanonicalKey(join, equalKey().toBuilder().setComparison(
                ComparisonJoinKey.ComparisonType.newBuilder().setSimple(
                        ComparisonJoinKey.SimpleComparisonType.SIMPLE_COMPARISON_TYPE_IS_NOT_DISTINCT_FROM))
                .build());
        addDeprecatedKey(join, "left_keys", field(0));
        addDeprecatedKey(join, "right_keys", field(1));

        assertRejected(join, relationField, "Deprecated join keys require canonical EQ comparison");
    }

    @Test
    @SuppressWarnings("deprecation")
    void normalizesFetchInsideSubqueryAndReusesUnchangedRelation() {
        Rel read = Rel.newBuilder().setRead(io.substrait.proto.ReadRel.getDefaultInstance()).build();
        var fetch = Rel.newBuilder().setFetch(FetchRel.newBuilder().setInput(read).setCount(3));
        var scalar = Expression.Subquery.Scalar.newBuilder().setInput(fetch);
        var expression = Expression.newBuilder().setSubquery(Expression.Subquery.newBuilder().setScalar(scalar));
        var projection = Rel.newBuilder().setProject(ProjectRel.newBuilder().setInput(read)
                .addExpressions(expression));
        var input = Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(
                RelRoot.newBuilder().setInput(projection))).build();

        var normalized = SubstraitProtoNormalizer.normalize(input);
        var project = normalized.getRelations(0).getRoot().getInput().getProject();
        assertThat(project.getInput()).isSameAs(read);
        var normalizedFetch = project.getExpressions(0).getSubquery().getScalar().getInput().getFetch();
        assertThat(normalizedFetch.getCountExpr().getLiteral().getI64()).isEqualTo(3);
        assertThat(normalizedFetch.getInput()).isSameAs(read);
        assertThat(SubstraitProtoNormalizer.normalize(normalized)).isSameAs(normalized);
    }

    private static void assertRejected(Message.Builder join, String relationField, String expectedMessage) {
        Plan input = nestedPlan(join.build(), relationField);
        assertThatThrownBy(() -> SubstraitProtoNormalizer.normalize(input))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }

    private static Message.Builder newJoin(String relationField) {
        return "hash_join".equals(relationField) ? HashJoinRel.newBuilder() : MergeJoinRel.newBuilder();
    }

    private static void addCanonicalKey(Message.Builder join, ComparisonJoinKey key) {
        join.addRepeatedField(descriptor(join, "keys"), key);
    }

    private static void addDeprecatedKey(Message.Builder join, String name, Expression.FieldReference key) {
        join.addRepeatedField(descriptor(join, name), key);
    }

    private static com.google.protobuf.Descriptors.FieldDescriptor descriptor(
            com.google.protobuf.MessageOrBuilder message, String name) {
        return message.getDescriptorForType().findFieldByName(name);
    }

    private static ComparisonJoinKey equalKey() {
        return ComparisonJoinKey.newBuilder().setLeft(field(0)).setRight(field(1))
                .setComparison(ComparisonJoinKey.ComparisonType.newBuilder().setSimple(
                        ComparisonJoinKey.SimpleComparisonType.SIMPLE_COMPARISON_TYPE_EQ)).build();
    }

    private static Expression.FieldReference field(int index) {
        return Expression.FieldReference.newBuilder().setRootReference(
                Expression.FieldReference.RootReference.getDefaultInstance()).setDirectReference(
                Expression.ReferenceSegment.newBuilder().setStructField(
                        Expression.ReferenceSegment.StructField.newBuilder().setField(index))).build();
    }

    private static Plan nestedPlan(Message join, String relationField) {
        Rel.Builder relation = Rel.newBuilder();
        relation.setField(descriptor(relation, relationField), join);
        return Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setProject(ProjectRel.newBuilder().setInput(relation))))).build();
    }

    private static Message getNestedJoin(Plan plan, String relationField) {
        Rel relation = plan.getRelations(0).getRoot().getInput().getProject().getInput();
        return (Message) relation.getField(descriptor(relation, relationField));
    }
}
