package org.pangrova.connector.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.connector.api.metadata.Constraint;
import org.pangrova.connector.api.metadata.Domain;
import org.pangrova.connector.api.metadata.table.ColumnHandle;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergConstraintExpressionsTest {
    private static final UUID ZERO = UUID.fromString("00000000-0000-0000-0000-000000000000");
    private static final UUID MATCH = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
    private static final UUID ALL = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.UUIDType.get()),
            Types.NestedField.required(2, "row_id", Types.LongType.get()));
    private static final ColumnHandle ID = () -> "id";
    private static final ColumnHandle ROW_ID = () -> "row_id";

    static Stream<Domain> uuidValueDomains() {
        return Stream.of(new Domain.Range(MATCH, MATCH, true, true),
                new Domain.Range(ZERO, ALL, false, false),
                new Domain.InList(Set.of(MATCH, ALL)));
    }

    @ParameterizedTest
    @MethodSource("uuidValueDomains")
    void ambiguousUuidBoundsCannotPruneMatchingFiles(Domain domain) {
        Constraint constraint = () -> Map.of(ID, domain);
        var expression = IcebergConstraintExpressions.fromConstraint(constraint, SCHEMA);
        // Canonical Parquet bounds and understated historical aggregation bounds
        // must both retain the file for Native row-level evaluation.
        assertThat(new InclusiveMetricsEvaluator(SCHEMA, expression, true).eval(file(ZERO, ALL, 1))).isTrue();
        assertThat(new InclusiveMetricsEvaluator(SCHEMA, expression, true).eval(file(ALL, ALL, 1))).isTrue();
    }

    @Test
    void reproducesSignedSdkPruningOfCanonicalParquetBounds() {
        assertThat(new InclusiveMetricsEvaluator(SCHEMA, Expressions.equal("id", MATCH), true)
                .eval(file(ZERO, ALL, 1))).isFalse();
    }

    @Test
    void safeNonUuidPredicatesStillPruneFiles() {
        Constraint constraint = () -> Map.of(ID, new Domain.Range(MATCH, MATCH, true, true),
                ROW_ID, new Domain.Range(10L, null, true, false));
        var expression = IcebergConstraintExpressions.fromConstraint(constraint, SCHEMA);
        assertThat(new InclusiveMetricsEvaluator(SCHEMA, expression, true).eval(file(ZERO, ALL, 1))).isFalse();
    }

    @Test
    void disjointBigintRangesSkipFilesBetweenThemAndKeepIndependentConstraints() {
        var ranges = new Domain.RangeUnion(List.of(new Domain.Range(1L, 2L, true, true),
                new Domain.Range(100L, 101L, true, true)));
        Constraint constraint = () -> Map.of(ROW_ID, ranges, ID, new Domain.IsNotNull());
        var expression = IcebergConstraintExpressions.fromConstraint(constraint, SCHEMA);
        var evaluator = new InclusiveMetricsEvaluator(SCHEMA, expression, true);
        assertThat(evaluator.eval(file(ZERO, ALL, 0, 0, 1))).isTrue();
        assertThat(evaluator.eval(file(ZERO, ALL, 0, 3, 99))).isFalse();
        assertThat(evaluator.eval(file(ZERO, ALL, 0, 99, 100))).isTrue();
        assertThat(evaluator.eval(file(ZERO, ALL, 4, 99, 100))).isFalse();
    }

    @Test
    void uuidNullCountsStillPruneFiles() {
        Constraint constraint = () -> Map.of(ID, new Domain.IsNull());
        var expression = IcebergConstraintExpressions.fromConstraint(constraint, SCHEMA);
        assertThat(new InclusiveMetricsEvaluator(SCHEMA, expression, true).eval(file(ZERO, ALL, 0))).isFalse();
    }

    private static DataFile file(UUID lower, UUID upper, long nulls) {
        return file(lower, upper, nulls, 1, 4);
    }

    private static DataFile file(UUID lower, UUID upper, long nulls, long lowId, long highId) {
        return DataFiles.builder(PartitionSpec.unpartitioned()).withPath("/data/uuid.parquet")
                .withFormat("parquet").withFileSizeInBytes(1024)
                .withMetrics(new Metrics(4L, null, Map.of(1, 4L, 2, 4L), Map.of(1, nulls, 2, 0L), null,
                        Map.of(1, Conversions.toByteBuffer(Types.UUIDType.get(), lower),
                                2, Conversions.toByteBuffer(Types.LongType.get(), lowId)),
                        Map.of(1, Conversions.toByteBuffer(Types.UUIDType.get(), upper),
                                2, Conversions.toByteBuffer(Types.LongType.get(), highId))))
                .build();
    }
}
