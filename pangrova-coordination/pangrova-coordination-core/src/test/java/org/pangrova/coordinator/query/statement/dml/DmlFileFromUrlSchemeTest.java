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
package org.pangrova.coordinator.query.statement.dml;

import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.expression.FunctionCall;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for F-INJECT-01: {@code FILE_FROM_URL} must reject URIs whose scheme reads
 * server-local / in-process resources (e.g. {@code file://}), so a SQL-supplied file reference
 * cannot be used for local file disclosure. Network/object-store schemes remain accepted.
 */
class DmlFileFromUrlSchemeTest {

    private static ColumnValue evalFileFromUrl(String url) {
        FunctionCall call = new FunctionCall(Identifier.of("FILE_FROM_URL"),
                List.<Expression>of(Literal.ofString(url)));
        return DmlExpressionEvaluator.extractLiteralColumnValue(call);
    }

    @Test
    void networkAndObjectStoreSchemesAreAccepted() {
        assertThat(evalFileFromUrl("s3://bucket/data/a.parquet"))
                .isInstanceOf(ColumnValue.RowValue.class);
        assertThat(evalFileFromUrl("https://host/path/a.json"))
                .isInstanceOf(ColumnValue.RowValue.class);
        assertThat(evalFileFromUrl("gs://bucket/obj"))
                .isInstanceOf(ColumnValue.RowValue.class);
    }

    @Test
    void fileSchemeIsRejected() {
        // Classic local-file-disclosure vector.
        assertThatThrownBy(() -> evalFileFromUrl("file:///etc/passwd"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scheme not allowed");
        // Case-insensitive.
        assertThatThrownBy(() -> evalFileFromUrl("FILE://host/x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scheme not allowed");
    }

    @Test
    void missingSchemeIsRejected() {
        assertThatThrownBy(() -> evalFileFromUrl("/local/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid URL");
    }
}
