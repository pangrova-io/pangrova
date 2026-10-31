package org.pangrova.connector.paimon;

import org.pangrova.connector.spi.metadata.DeleteFilter;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Capability rejection must precede even builder creation and leave snapshots and data unchanged. */
class PaimonDeleteByFilterRejectionTest extends PaimonDeleteTestSupport {

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupportedFilters")
    void unsupportedPredicatesNeverOpenOperations(String description, DeleteFilter filter) throws Exception {
        createTable(true, false);
        writeRows(new StoredRow(1, "east", "one", "target"),
                new StoredRow(2, "west", "two", "untouched"));

        assertRejectedWithoutMutation(filter);
    }

    static Stream<Arguments> unsupportedFilters() {
        return Stream.concat(scalarFilters(), logicalFilters());
    }

    private static Stream<Arguments> scalarFilters() {
        return Stream.of(
                Arguments.of("nonpartition data", new DeleteFilter.Equal("payload", "target")),
                Arguments.of("primary key is not a partition", new DeleteFilter.Equal("id", "1")),
                Arguments.of("unknown column", new DeleteFilter.Equal("missing", "east")),
                Arguments.of("null column", new DeleteFilter.Equal(null, "east")),
                Arguments.of("empty column", new DeleteFilter.Equal("", "east")),
                Arguments.of("not equal", new DeleteFilter.NotEqual("region", "east")),
                Arguments.of("less than", new DeleteFilter.LessThan("region", "west")),
                Arguments.of("less than or equal", new DeleteFilter.LessThanOrEqual("region", "east")),
                Arguments.of("greater than", new DeleteFilter.GreaterThan("region", "a")),
                Arguments.of("greater than or equal", new DeleteFilter.GreaterThanOrEqual("region", "east")),
                Arguments.of("null equality", new DeleteFilter.Equal("region", null)),
                Arguments.of("IS NULL", new DeleteFilter.IsNull("region")),
                Arguments.of("IS NOT NULL", new DeleteFilter.IsNotNull("region")),
                Arguments.of("IN", new DeleteFilter.In("region", List.of("east", "west"))),
                Arguments.of("empty string", new DeleteFilter.Equal("region", "")),
                Arguments.of("integer coercion", new DeleteFilter.Equal("region", 1)),
                Arguments.of("long coercion", new DeleteFilter.Equal("region", Long.MAX_VALUE)),
                Arguments.of("negative coercion", new DeleteFilter.Equal("region", Integer.MIN_VALUE)),
                Arguments.of("double coercion", new DeleteFilter.Equal("region", 1.0d)),
                Arguments.of("NaN coercion", new DeleteFilter.Equal("region", Double.NaN)),
                Arguments.of("decimal coercion", new DeleteFilter.Equal("region", new BigDecimal("1.00"))),
                Arguments.of("overflow coercion", new DeleteFilter.Equal("region",
                        new BigInteger("9223372036854775808"))),
                Arguments.of("boolean coercion", new DeleteFilter.Equal("region", true)),
                Arguments.of("SDK string is not Java String", new DeleteFilter.Equal("region", text("east"))));
    }

    private static Stream<Arguments> logicalFilters() {
        DeleteFilter east = new DeleteFilter.Equal("region", "east");
        DeleteFilter data = new DeleteFilter.Equal("payload", "target");
        return Stream.of(
                Arguments.of("OR", new DeleteFilter.Or(List.of(east, new DeleteFilter.Equal("region", "west")))),
                Arguments.of("single OR", new DeleteFilter.Or(List.of(east))),
                Arguments.of("empty OR", new DeleteFilter.Or(List.of())),
                Arguments.of("NOT", new DeleteFilter.Not(east)),
                Arguments.of("mixed partition then data", and(east, data)),
                Arguments.of("mixed data then partition", and(data, east)),
                Arguments.of("mixed range", and(east, new DeleteFilter.GreaterThan("day", "one"))),
                Arguments.of("mixed null", and(east, new DeleteFilter.Equal("day", null))),
                Arguments.of("mixed empty value", and(east, new DeleteFilter.Equal("day", ""))),
                Arguments.of("mixed OR", and(east, new DeleteFilter.Or(List.of(data)))),
                Arguments.of("mixed NOT", and(east, new DeleteFilter.Not(data))),
                Arguments.of("mixed All", and(east, new DeleteFilter.All())),
                Arguments.of("empty AND", and()),
                Arguments.of("nested empty AND", and(east, and())),
                Arguments.of("conflicting equalities", and(east, new DeleteFilter.Equal("region", "west"))),
                Arguments.of("conflicting case-insensitive keys", and(east,
                        and(new DeleteFilter.Equal("REGION", "west")))),
                Arguments.of("different value case is a conflict", and(east,
                        new DeleteFilter.Equal("REGION", "EAST"))),
                Arguments.of("mixed numeric value", and(east, new DeleteFilter.Equal("day", 1))));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stringDataEqualityIsNotPartitionEquality(boolean appendOnly) throws Exception {
        createTable(false, appendOnly);
        writeRows(new StoredRow(1, "east", "one", "target"));

        assertRejectedWithoutMutation(new DeleteFilter.Equal("region", "east"));
    }

    @ParameterizedTest(name = "numeric partition: {0}")
    @MethodSource("numericPartitionFilters")
    void numericPartitionsNeverUseStringFormattingAsSqlCoercion(DeleteFilter filter) throws Exception {
        createTable(rowSchema().column("number", DataTypes.INT()).partitionKeys("number")
                .primaryKey("id", "number").option("bucket", "1").build());
        writeRecords(GenericRow.of(1L, text("east"), text("one"), text("target"), 1),
                GenericRow.of(2L, text("west"), text("two"), text("minimum"), Integer.MIN_VALUE),
                GenericRow.of(3L, text("west"), text("two"), text("maximum"), Integer.MAX_VALUE));

        assertRejectedWithoutMutation(filter);
    }

    static Stream<DeleteFilter> numericPartitionFilters() {
        return Stream.of(
                new DeleteFilter.Equal("number", "1"),
                new DeleteFilter.Equal("number", "01"),
                new DeleteFilter.Equal("number", "1.0"),
                new DeleteFilter.Equal("number", "2147483648"),
                new DeleteFilter.Equal("number", "-2147483648"),
                new DeleteFilter.Equal("number", 1),
                new DeleteFilter.Equal("number", 1L),
                new DeleteFilter.Equal("number", new BigDecimal("1.0")));
    }

    @ParameterizedTest(name = "default partition marker: {0}")
    @MethodSource("partitionMarkers")
    void defaultPartitionMarkerCannotBeMistakenForLiteralEquality(String marker) throws Exception {
        Map<String, String> options = marker.equals(CoreOptions.PARTITION_DEFAULT_NAME.defaultValue())
            ? Map.of() : Map.of(CoreOptions.PARTITION_DEFAULT_NAME.key(), marker);
        createTable(true, true, options);
        writeRows(new StoredRow(1, null, "one", "null-partition"),
                new StoredRow(2, "east", "one", "unrelated-partition"),
                new StoredRow(3, "east", null, "null-second-key"));

        assertRejectedWithoutMutation(new DeleteFilter.Equal("region", marker));
        assertRejectedWithoutMutation(and(new DeleteFilter.Equal("region", "east"),
                new DeleteFilter.Equal("day", marker)));
        assertRejectedWithoutMutation(new DeleteFilter.Equal("region", null));
        assertRejectedWithoutMutation(new DeleteFilter.IsNull("region"));
        assertRejectedWithoutMutation(new DeleteFilter.Equal("region", ""));
    }

    static Stream<String> partitionMarkers() {
        return Stream.of(CoreOptions.PARTITION_DEFAULT_NAME.defaultValue(), "__CUSTOM_NULL__");
    }

    @Test
    void nullFilterFailsBeforeOpeningOperations() throws Exception {
        createTable(true, false);
        writeRows(new StoredRow(1, "east", "one", "unchanged"));
        var guarded = PaimonDeleteGuardTable.rejectOperations(freshTable());
        List<StoredRow> before = readRows();
        long snapshot = snapshotId();

        assertThatThrownBy(() -> PaimonDeleteByFilterExecutor.execute(guarded, null, DATABASE, TABLE_NAME))
                .isExactlyInstanceOf(NullPointerException.class)
                .hasMessage("filter");

        assertThat(snapshotId()).isEqualTo(snapshot);
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(before);
    }

    private void assertRejectedWithoutMutation(DeleteFilter filter) throws Exception {
        var guarded = PaimonDeleteGuardTable.rejectOperations(freshTable());
        List<StoredRow> before = readRows();
        long snapshot = snapshotId();

        assertThatThrownBy(() -> PaimonDeleteByFilterExecutor.execute(guarded, filter, DATABASE, TABLE_NAME))
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("requires distributed execution")
                .hasMessageContaining(DATABASE + "." + TABLE_NAME);

        assertThat(snapshotId()).isEqualTo(snapshot);
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(before);
    }

    private static DeleteFilter and(DeleteFilter... filters) {
        return new DeleteFilter.And(List.of(filters));
    }
}