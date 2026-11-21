package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.extension.AdvancedExtension;
import io.substrait.hint.Hint;
import io.substrait.relation.Rel;
import io.substrait.relation.physical.ComparisonJoinKey;
import io.substrait.relation.physical.MergeJoin;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Compatibility wrapper for the Substrait Java 0.95 MergeJoin record-type bug.
 *
 * <p>The upstream {@link MergeJoin} implementation derives both sides from the
 * right input. That corrupts schemas whenever the inputs are asymmetric. Keep
 * the official relation and visitor contract, but derive the standard output
 * type locally until the upstream implementation is fixed and verified.</p>
 */
final class StandardsCompliantMergeJoin extends MergeJoin {

    private final MergeJoin delegate;

    private StandardsCompliantMergeJoin(MergeJoin delegate) {
        this.delegate = delegate;
    }

    static MergeJoin wrap(MergeJoin delegate) {
        return new StandardsCompliantMergeJoin(delegate);
    }

    @Override
    public List<ComparisonJoinKey> getKeys() {
        return delegate.getKeys();
    }

    @Override
    public JoinType getJoinType() {
        return delegate.getJoinType();
    }

    @Override
    public Optional<Expression> getPostJoinFilter() {
        return delegate.getPostJoinFilter();
    }

    @Override
    public Rel getLeft() {
        return delegate.getLeft();
    }

    @Override
    public Rel getRight() {
        return delegate.getRight();
    }

    @Override
    public Optional<AdvancedExtension> getExtension() {
        return delegate.getExtension();
    }

    @Override
    public Optional<Rel.Remap> getRemap() {
        return delegate.getRemap();
    }

    @Override
    public Optional<AdvancedExtension> getCommonExtension() {
        return delegate.getCommonExtension();
    }

    @Override
    public Optional<Hint> getHint() {
        return delegate.getHint();
    }

    @Override
    protected Type.Struct deriveRecordType() {
        Stream<Type> left = getLeft().getRecordType().fields().stream();
        Stream<Type> right = getRight().getRecordType().fields().stream();
        return switch (getJoinType()) {
            case INNER -> TypeCreator.REQUIRED.struct(Stream.concat(left, right));
            case LEFT -> TypeCreator.REQUIRED.struct(Stream.concat(
                    left, right.map(type -> type.withNullable(true))));
            case RIGHT -> TypeCreator.REQUIRED.struct(Stream.concat(
                    left.map(type -> type.withNullable(true)), right));
            case OUTER -> TypeCreator.REQUIRED.struct(Stream.concat(
                    left.map(type -> type.withNullable(true)),
                    right.map(type -> type.withNullable(true))));
            case LEFT_SEMI, LEFT_ANTI -> TypeCreator.REQUIRED.struct(left);
            case RIGHT_SEMI, RIGHT_ANTI -> TypeCreator.REQUIRED.struct(right);
            case UNKNOWN -> throw new IllegalStateException(
                    "MergeJoin output type cannot be derived for UNKNOWN join type");
        };
    }
}
