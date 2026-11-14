/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.streaming.cdc;

import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.enums.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.junit.jupiter.api.Test;
import org.pangrova.streaming.api.format.CdcFormatFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CdcRecordConverterRegistryTest {

    @Test
    void getReturnsRegisteredConverter() {
        CdcRecordConverter converter = stubConverter("debezium-json");
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(List.of(converter));

        assertThat(registry.get("debezium-json")).isSameAs(converter);
    }

    @Test
    void getThrowsForUnknownFormat() {
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(List.of(stubConverter("debezium-json")));

        assertThatThrownBy(() -> registry.get("canal-json"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported source format: canal-json")
                .hasMessageContaining("debezium-json");
    }

    @Test
    void supportedFormatsReturnsAllRegisteredIds() {
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(
                List.of(stubConverter("debezium-json"), stubConverter("canal-json")));

        assertThat(registry.supportedFormats()).isEqualTo(Set.of("debezium-json", "canal-json"));
    }

    @Test
    void emptyRegistryHasNoFormats() {
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(List.of());

        assertThat(registry.supportedFormats()).isEmpty();
    }

    @Test
    void duplicateFormatIdIsRejected() {
        CdcRecordConverter first = stubConverter("debezium-json");
        CdcRecordConverter second = stubConverter("debezium-json");
        assertThatThrownBy(() -> new CdcRecordConverterRegistry(List.of(first, second)))
                .hasMessageContaining("duplicate CDC format");
    }

    @Test
    void registryIsImmutableAfterConstruction() {
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(
                List.of(stubConverter("debezium-json")));

        assertThatThrownBy(() -> registry.supportedFormats().add("hacked"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void invalidFactoryIdentifiersAreRejectedBeforeAnyDecoderIsCreated() {
        Supplier<CdcRecordConverter> neverCreate = () -> {
            throw new AssertionError("Created a decoder before validating all factory identifiers");
        };
        var first = factory("json", neverCreate);
        assertThatThrownBy(() -> CdcRecordConverterRegistry.fromFactories(List.of(first, factory("json", neverCreate))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("duplicate CDC format");
        for (String invalid : java.util.Arrays.asList("", " ", null)) {
            assertThatThrownBy(() -> CdcRecordConverterRegistry.fromFactories(List.of(first, factory(invalid, neverCreate))))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Blank");
        }
    }

    @Test
    void decoderMustExistAndMatchItsFactory() {
        for (CdcRecordConverter invalid : java.util.Arrays.asList(null, stubConverter("another-format"))) {
            assertThatThrownBy(() -> CdcRecordConverterRegistry.fromFactories(List.of(factory("json", () -> invalid))))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("json");
        }
    }

    private static CdcFormatFactory factory(String id, Supplier<CdcRecordConverter> creator) {
        return factory(id, CdcFormatFactory.API_VERSION, creator);
    }

    @Test
    void incompatibleFormatRejectsAdmissionBeforeAnyDecoderIsCreated() {
        Supplier<CdcRecordConverter> unexpected = () -> { throw new AssertionError("Decoder created"); };
        assertThatThrownBy(() -> CdcRecordConverterRegistry.fromFactories(
                List.of(factory("valid", unexpected), factory("future", 2, unexpected))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Incompatible CDC format factory: future");
    }

    private static CdcFormatFactory factory(String id, int version, Supplier<CdcRecordConverter> creator) {
        return new CdcFormatFactory() {
            @Override public int apiVersion() { return version; }
            @Override public String identifier() { return id; }
            @Override public CdcRecordConverter create() { return creator.get(); }
        };
    }

    private static CdcRecordConverter stubConverter(String formatId) {
        return new CdcRecordConverter() {
            @Override
            public String formatId() {
                return formatId;
            }

            @Override
            public CdcEnvelope deserialize(byte[] payload) {
                return new CdcEnvelope(
                        CdcOperation.INSERT,
                        "test-table",
                        null,
                        CdcRow.of(Map.of()),
                        Instant.now(),
                        0,
                        Map.of());
            }
        };
    }
}
