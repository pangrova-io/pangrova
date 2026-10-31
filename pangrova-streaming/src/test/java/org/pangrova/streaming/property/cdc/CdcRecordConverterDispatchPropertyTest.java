package org.pangrova.streaming.property.cdc;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.pangrova.streaming.core.cdc.CdcRecordConverterRegistry;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Property 22: CdcRecordConverter format dispatch correctness.
 *
 * <p>Validates: Requirements 1.5, 8.4</p>
 *
 * <p>For any registry containing converters with format IDs {F1, F2, ..., Fn},
 * looking up Fi returns the converter with formatId() == Fi. Looking up an
 * unregistered format ID throws IllegalArgumentException.</p>
 */
class CdcRecordConverterDispatchPropertyTest {

    /**
     * Validates: Requirements 1.5
     *
     * For any set of registered format IDs, registry.get(id) returns the
     * converter whose formatId() matches the requested id.
     */
    @Property(tries = 10)
    void registeredFormatReturnsCorrectConverter(
            @ForAll("distinctFormatIds") List<String> formatIds) {
        Assume.that(!formatIds.isEmpty());

        List<CdcRecordConverter> converters = formatIds.stream()
                .map(CdcRecordConverterDispatchPropertyTest::stubConverter)
                .toList();
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(converters);

        for (int i = 0; i < formatIds.size(); i++) {
            String id = formatIds.get(i);
            CdcRecordConverter result = registry.get(id);
            assertThat(result.formatId()).isEqualTo(id);
            assertThat(result).isSameAs(converters.get(i));
        }
    }

    /**
     * Validates: Requirements 8.4
     *
     * Looking up an unregistered format ID throws IllegalArgumentException.
     */
    @Property(tries = 10)
    void unregisteredFormatThrowsIllegalArgument(
            @ForAll("distinctFormatIds") List<String> registeredIds,
            @ForAll("formatId") String queryId) {
        Assume.that(!registeredIds.contains(queryId));

        List<CdcRecordConverter> converters = registeredIds.stream()
                .map(CdcRecordConverterDispatchPropertyTest::stubConverter)
                .toList();
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(converters);

        assertThatThrownBy(() -> registry.get(queryId))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Validates: Requirements 1.5
     *
     * supportedFormats() returns exactly the set of registered format IDs.
     */
    @Property(tries = 10)
    void supportedFormatsMatchesRegisteredIds(
            @ForAll("distinctFormatIds") List<String> formatIds) {
        List<CdcRecordConverter> converters = formatIds.stream()
                .map(CdcRecordConverterDispatchPropertyTest::stubConverter)
                .toList();
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(converters);

        Set<String> expected = Set.copyOf(formatIds);
        assertThat(registry.supportedFormats()).isEqualTo(expected);
    }

    /**
     * Validates: Requirements 1.5
     *
     * When duplicate format IDs are registered, last-one-wins semantics apply.
     */
    @Property(tries = 10)
    void duplicateFormatIdLastOneWins(
            @ForAll("formatId") String formatId,
            @ForAll @IntRange(min = 2, max = 5) int count) {
        List<CdcRecordConverter> converters = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            converters.add(stubConverter(formatId));
        }
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(converters);

        CdcRecordConverter result = registry.get(formatId);
        assertThat(result).isSameAs(converters.get(count - 1));
    }

    @Provide
    Arbitrary<List<String>> distinctFormatIds() {
        return formatId()
                .list()
                .ofMinSize(0)
                .ofMaxSize(8)
                .uniqueElements();
    }

    @Provide
    Arbitrary<String> formatId() {
        Arbitrary<String> prefix = Arbitraries.of(
                "debezium", "canal", "maxwell", "avro", "proto", "custom", "json", "flat");
        Arbitrary<String> suffix = Arbitraries.of(
                "-json", "-avro", "-proto", "-binary", "-text", "-v2", "-compact");
        return Combinators.combine(prefix, suffix).as((p, s) -> p + s);
    }

    private static CdcRecordConverter stubConverter(String id) {
        return new CdcRecordConverter() {
            @Override
            public String formatId() {
                return id;
            }

            @Override
            public CdcEnvelope deserialize(byte[] payload) {
                return new CdcEnvelope(
                        CdcOperation.INSERT,
                        "stub-table",
                        null,
                        CdcRow.of(Map.of()),
                        Instant.EPOCH,
                        0,
                        Map.of());
            }
        };
    }
}
