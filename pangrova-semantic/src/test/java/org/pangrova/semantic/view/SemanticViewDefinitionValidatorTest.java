/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.semantic.view;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SemanticViewDefinitionValidatorTest {

    private final SemanticViewDefinitionValidator validator =
            new SemanticViewDefinitionValidator();

    @Test
    void canonicalizesFieldOrderAndExtractsFullyQualifiedSources() {
        var validated = validator.validate(
                "{\"measures\":[],\"logicalTables\":[{"
                        + "\"source\":\"lake.sales.orders\",\"name\":\"orders\"}]}");

        assertThat(validated.canonicalJson()).isEqualTo(
                "{\"logicalTables\":[{\"name\":\"orders\","
                        + "\"source\":\"lake.sales.orders\"}],\"measures\":[]}");
        assertThat(validated.sha256()).hasSize(64);
        assertThat(validated.sources()).singleElement().satisfies(source -> {
            assertThat(source.catalog()).isEqualTo("lake");
            assertThat(source.schema()).isEqualTo("sales");
            assertThat(source.table()).isEqualTo("orders");
        });
    }

    @Test
    void rejectsMalformedUnboundedAndAmbiguousDefinitions() {
        assertInvalid("not-json", "not valid JSON");
        assertInvalid("{}", "requires logicalTables");
        assertInvalid(
                "{\"logicalTables\":[{\"name\":\"orders\","
                        + "\"source\":\"orders\"}]}",
                "catalog.schema.table");
        assertInvalid(
                "{\"logicalTables\":[{\"name\":\"orders\","
                        + "\"source\":\"lake.sales.orders\"}],\"measures\":{}}",
                "must be an array");
        assertInvalid("{".repeat(SemanticViewDefinitionValidator.MAX_DEFINITION_BYTES + 1),
                "exceeds 1 MiB");
    }

    private void assertInvalid(String json, String message) {
        assertThatThrownBy(() -> validator.validate(json))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining(message)
                .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE));
    }
}
