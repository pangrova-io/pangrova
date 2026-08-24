package org.pangrova.streaming.aggregation;

import org.pangrova.spi.connector.data.ColumnValue;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests streaming AVG with retraction semantics.
 * Verifies that INSERT/DELETE operations produce correct running averages.
 */
class StreamingAvgRetractionTest {

    @Test
    void avgAccumulatesInsertsCorrectly() {
        AvgAccumulator avg = new AvgAccumulator();

        avg.accumulate(new ColumnValue.IntValue(10));
        avg.accumulate(new ColumnValue.IntValue(20));
        avg.accumulate(new ColumnValue.IntValue(30));

        ColumnValue result = avg.result();
        assertThat(result).isInstanceOf(ColumnValue.DoubleValue.class);
        assertThat(((ColumnValue.DoubleValue) result).value()).isCloseTo(20.0, within(0.001));
    }

    @Test
    void avgRetractsDeletesCorrectly() {
        AvgAccumulator avg = new AvgAccumulator();

        // Insert 10, 20, 30 → avg = 20
        avg.accumulate(new ColumnValue.IntValue(10));
        avg.accumulate(new ColumnValue.IntValue(20));
        avg.accumulate(new ColumnValue.IntValue(30));

        // Retract 10 → remaining: 20, 30 → avg = 25
        avg.retract(new ColumnValue.IntValue(10));

        ColumnValue result = avg.result();
        assertThat(((ColumnValue.DoubleValue) result).value()).isCloseTo(25.0, within(0.001));
        assertThat(avg.count()).isEqualTo(2);
    }

    @Test
    void avgHandlesUpdateAsRetractThenAccumulate() {
        AvgAccumulator avg = new AvgAccumulator();

        // Insert 10, 20 → avg = 15
        avg.accumulate(new ColumnValue.IntValue(10));
        avg.accumulate(new ColumnValue.IntValue(20));

        // UPDATE: old value 10 → new value 40 (retract old, accumulate new)
        avg.retract(new ColumnValue.IntValue(10));
        avg.accumulate(new ColumnValue.IntValue(40));

        // Remaining: 20, 40 → avg = 30
        ColumnValue result = avg.result();
        assertThat(((ColumnValue.DoubleValue) result).value()).isCloseTo(30.0, within(0.001));
        assertThat(avg.count()).isEqualTo(2);
    }

    @Test
    void avgReturnsNullWhenAllRetracted() {
        AvgAccumulator avg = new AvgAccumulator();

        avg.accumulate(new ColumnValue.IntValue(10));
        avg.accumulate(new ColumnValue.IntValue(20));

        avg.retract(new ColumnValue.IntValue(10));
        avg.retract(new ColumnValue.IntValue(20));

        assertThat(avg.result()).isInstanceOf(ColumnValue.NullValue.class);
        assertThat(avg.count()).isEqualTo(0);
    }

    @Test
    void avgIgnoresNullValues() {
        AvgAccumulator avg = new AvgAccumulator();

        avg.accumulate(new ColumnValue.IntValue(10));
        avg.accumulate(new ColumnValue.NullValue());
        avg.accumulate(new ColumnValue.IntValue(30));

        // Only 10 and 30 counted → avg = 20
        ColumnValue result = avg.result();
        assertThat(((ColumnValue.DoubleValue) result).value()).isCloseTo(20.0, within(0.001));
        assertThat(avg.count()).isEqualTo(2);
    }

    @Test
    void avgHandlesMixedNumericTypes() {
        AvgAccumulator avg = new AvgAccumulator();

        avg.accumulate(new ColumnValue.IntValue(10));
        avg.accumulate(new ColumnValue.LongValue(20L));
        avg.accumulate(new ColumnValue.DoubleValue(30.0));

        ColumnValue result = avg.result();
        assertThat(((ColumnValue.DoubleValue) result).value()).isCloseTo(20.0, within(0.001));
    }

    @Test
    void avgResetClearsState() {
        AvgAccumulator avg = new AvgAccumulator();

        avg.accumulate(new ColumnValue.IntValue(100));
        avg.accumulate(new ColumnValue.IntValue(200));

        avg.reset();

        assertThat(avg.result()).isInstanceOf(ColumnValue.NullValue.class);
        assertThat(avg.count()).isEqualTo(0);
        assertThat(avg.sum()).isEqualTo(0.0);
    }

    @Test
    void avgWithRetractionProducesCorrectStreamingResults() {
        // Simulates a streaming scenario:
        // Window 1 emits: INSERT 5, INSERT 15, INSERT 25 → avg = 15
        // Retraction from upstream: DELETE 15 → avg = 15 (remaining: 5, 25)
        // New insert: INSERT 35 → avg = (5+25+35)/3 = 21.67
        AvgAccumulator avg = new AvgAccumulator();

        avg.accumulate(new ColumnValue.IntValue(5));
        avg.accumulate(new ColumnValue.IntValue(15));
        avg.accumulate(new ColumnValue.IntValue(25));
        assertThat(((ColumnValue.DoubleValue) avg.result()).value()).isCloseTo(15.0, within(0.001));

        // Upstream retracts the 15
        avg.retract(new ColumnValue.IntValue(15));
        assertThat(((ColumnValue.DoubleValue) avg.result()).value()).isCloseTo(15.0, within(0.001));

        // New value arrives
        avg.accumulate(new ColumnValue.IntValue(35));
        assertThat(((ColumnValue.DoubleValue) avg.result()).value())
                .isCloseTo(21.666, within(0.01));
        assertThat(avg.count()).isEqualTo(3);
    }
}
