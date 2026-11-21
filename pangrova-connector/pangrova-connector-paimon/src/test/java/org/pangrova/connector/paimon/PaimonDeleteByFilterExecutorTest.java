package org.pangrova.connector.paimon;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.DeleteFilter;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.apache.paimon.catalog.Catalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises real manifest-only deletes, including append-only tables and metadata wiring. */
class PaimonDeleteByFilterExecutorTest extends PaimonDeleteTestSupport {

    private static final StoredRow EAST_ONE = new StoredRow(1, "east", "one", "east-first");
    private static final StoredRow EAST_TWO = new StoredRow(2, "east", "two", "east-second");
    private static final StoredRow WEST_ONE = new StoredRow(3, "west", "one", "west-first");
    private static final StoredRow WEST_TWO = new StoredRow(4, "west", "two", "west-second");
    private static final StoredRow UPPER_EAST = new StoredRow(5, "EAST", "one", "case-sensitive-value");
    private static final List<StoredRow> INITIAL_ROWS =
            List.of(EAST_ONE, EAST_TWO, WEST_ONE, WEST_TWO, UPPER_EAST);

    @ParameterizedTest(name = "All: partitioned={0}, appendOnly={1}, empty={2}")
    @CsvSource({"false,false,false", "false,false,true", "false,true,false", "false,true,true",
            "true,false,false", "true,false,true", "true,true,false", "true,true,true"})
    void allDeletesWithoutCoordinatorReadsOrWrites(boolean partitioned, boolean appendOnly, boolean empty)
            throws Exception {
        createTable(partitioned, appendOnly);
        if (!empty) {
            writeRows(INITIAL_ROWS.toArray(StoredRow[]::new));
        }

        execute(new DeleteFilter.All());
        assertThat(readRows()).isEmpty();
        execute(new DeleteFilter.All());
        assertThat(readRows()).isEmpty();

        writeRows(WEST_ONE);
        assertThat(readRows()).containsExactly(WEST_ONE);
    }

    @ParameterizedTest(name = "{0}, appendOnly={1}")
    @MethodSource("partitionFilters")
    void partitionSpecsDeleteOnlyMatchingPartitions(String description, boolean appendOnly,
                                                    DeleteFilter filter, List<StoredRow> remaining)
            throws Exception {
        createTable(true, appendOnly);
        writeRows(INITIAL_ROWS.toArray(StoredRow[]::new));

        execute(filter);
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(remaining);

        execute(filter);
        assertThat(readRows()).containsExactlyInAnyOrderElementsOf(remaining);
    }

    static Stream<Arguments> partitionFilters() {
        return Stream.of(false, true).flatMap(appendOnly -> Stream.of(
                Arguments.of("VARCHAR equality", appendOnly, equal("region", "east"),
                        List.of(WEST_ONE, WEST_TWO, UPPER_EAST)),
                Arguments.of("partial AND", appendOnly, and(equal("REGION", "east")),
                        List.of(WEST_ONE, WEST_TWO, UPPER_EAST)),
                Arguments.of("non-prefix partial AND", appendOnly, and(equal("day", "one")),
                        List.of(EAST_TWO, WEST_TWO)),
                Arguments.of("full AND", appendOnly, and(equal("region", "east"), equal("day", "one")),
                        List.of(EAST_TWO, WEST_ONE, WEST_TWO, UPPER_EAST)),
                Arguments.of("nested reversed AND", appendOnly,
                        and(equal("DAY", "one"), and(equal("Region", "east"))),
                        List.of(EAST_TWO, WEST_ONE, WEST_TWO, UPPER_EAST)),
                Arguments.of("identical equalities", appendOnly,
                        and(equal("region", "east"), equal("REGION", "east")),
                        List.of(WEST_ONE, WEST_TWO, UPPER_EAST)),
                Arguments.of("no matching region", appendOnly, equal("region", "missing"), INITIAL_ROWS),
                Arguments.of("no matching full spec", appendOnly,
                        and(equal("region", "east"), equal("day", "missing")), INITIAL_ROWS)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void partitionDeleteOnNeverWrittenTableIsRepeatable(boolean appendOnly) throws Exception {
        createTable(true, appendOnly);

        execute(and(equal("region", "east"), equal("day", "one")));
        assertThat(readRows()).isEmpty();
        execute(equal("region", "east"));
        assertThat(readRows()).isEmpty();
    }

    @Test
    void unrelatedPartitionsRetainUpsertsAndRemainWritable() throws Exception {
        createTable(true, false);
        writeRows(INITIAL_ROWS.toArray(StoredRow[]::new));
        StoredRow updated = new StoredRow(3, "west", "one", "updated-before-delete");
        StoredRow sameIdElsewhere = new StoredRow(1, "west", "two", "same-id-other-partition");
        writeRows(updated, sameIdElsewhere);

        execute(equal("region", "east"));
        assertThat(readRows()).containsExactlyInAnyOrder(updated, sameIdElsewhere, WEST_TWO, UPPER_EAST);

        StoredRow updatedAgain = new StoredRow(3, "west", "one", "updated-after-delete");
        StoredRow reinserted = new StoredRow(1, "east", "one", "reinserted-after-delete");
        writeRows(updatedAgain, reinserted);
        assertThat(readRows()).containsExactlyInAnyOrder(
                updatedAgain, sameIdElsewhere, WEST_TWO, UPPER_EAST, reinserted);
    }

    @Test
    void appendOnlyDeletePreservesDuplicateRowsInUnrelatedPartitions() throws Exception {
        createTable(true, true);
        writeRows(EAST_ONE, WEST_ONE, WEST_ONE);

        execute(equal("region", "east"));

        assertThat(readRows()).containsExactlyInAnyOrder(WEST_ONE, WEST_ONE);
    }

        @Test
        void varcharNumericTextUsesLiteralEqualityWithoutNumericCoercion() throws Exception {
                createTable(true, false);
                StoredRow leadingZero = new StoredRow(1, "01", "one", "leading-zero");
                StoredRow integerText = new StoredRow(2, "1", "one", "integer-text");
                StoredRow decimalText = new StoredRow(3, "1.0", "one", "decimal-text");
                writeRows(leadingZero, integerText, decimalText);

                execute(equal("region", "1"));

                assertThat(readRows()).containsExactlyInAnyOrder(leadingZero, decimalText);
        }

    @Test
    void metadataWiringUsesFreshTableAfterHandleCreation() throws Exception {
        createTable(true, false);
        writeRows(EAST_ONE, WEST_ONE);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var handle = metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, TABLE_NAME));
        assertThat(handle).isNotNull();
        StoredRow updatedWest = new StoredRow(3, "west", "one", "latest-west");
        writeRows(EAST_TWO, updatedWest);

        metadata.deleteByFilter(SESSION, handle, equal("region", "east"));
        assertThat(readRows()).containsExactly(updatedWest);

        metadata.deleteByFilter(SESSION, handle, new DeleteFilter.All());
        assertThat(readRows()).isEmpty();
    }

    @Test
    void metadataPropagatesUnsupportedPredicateWithoutMutating() throws Exception {
        createTable(true, false);
        writeRows(EAST_ONE, WEST_ONE);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var handle = metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, TABLE_NAME));
        long before = snapshotId();

        assertThatThrownBy(() -> metadata.deleteByFilter(SESSION, handle, equal("payload", "east-first")))
                .isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("distributed execution");

        assertThat(snapshotId()).isEqualTo(before);
        assertThat(readRows()).containsExactlyInAnyOrder(EAST_ONE, WEST_ONE);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void metadataPropagatesMissingTableBeforeCapabilityDecision(boolean unsupportedFilter) throws Exception {
        createTable(true, false);
        var metadata = new PaimonMetadata(catalog, "delete_catalog", new org.pangrova.connector.paimon.PaimonSplitRegistry());
        var handle = metadata.getTableHandle(SESSION, new SchemaTableName(DATABASE, TABLE_NAME));
        assertThat(handle).isNotNull();
        catalog.dropTable(IDENTIFIER, false);
        DeleteFilter filter = unsupportedFilter ? equal("payload", "anything") : new DeleteFilter.All();

        assertThatExceptionOfType(ConnectorException.class)
                .isThrownBy(() -> metadata.deleteByFilter(SESSION, handle, filter))
                .withMessageContaining("Table not found")
                .withMessageContaining(TABLE_NAME)
                .withCauseInstanceOf(Catalog.TableNotExistException.class)
                .satisfies(error -> assertThat(error.getErrorCode())
                        .isEqualTo(PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND));
    }

    private void execute(DeleteFilter filter) throws Exception {
        PaimonDeleteByFilterExecutor.execute(
                new PaimonDeleteGuardTable(freshTable()), filter, DATABASE, TABLE_NAME);
    }

    private static DeleteFilter equal(String column, String value) {
        return new DeleteFilter.Equal(column, value);
    }

    private static DeleteFilter and(DeleteFilter... filters) {
        return new DeleteFilter.And(List.of(filters));
    }
}
