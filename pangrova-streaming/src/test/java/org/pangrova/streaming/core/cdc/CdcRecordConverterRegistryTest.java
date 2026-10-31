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
package org.pangrova.streaming.core.cdc;

import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.cdc.CdcRow;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    void duplicateFormatIdLastOneWins() {
        CdcRecordConverter first = stubConverter("debezium-json");
        CdcRecordConverter second = stubConverter("debezium-json");
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(List.of(first, second));

        assertThat(registry.get("debezium-json")).isSameAs(second);
    }

    @Test
    void registryIsImmutableAfterConstruction() {
        CdcRecordConverterRegistry registry = new CdcRecordConverterRegistry(
                List.of(stubConverter("debezium-json")));

        assertThatThrownBy(() -> registry.supportedFormats().add("hacked"))
                .isInstanceOf(UnsupportedOperationException.class);
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
