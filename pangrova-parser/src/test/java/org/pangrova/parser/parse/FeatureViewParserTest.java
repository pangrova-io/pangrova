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
package org.pangrova.parser.parse;

import org.pangrova.parser.format.AstPrinter;

import org.junit.jupiter.api.Test;
import org.pangrova.parser.api.enums.StatementType;
import org.pangrova.parser.api.ast.statement.CreateFeatureViewStatement;
import org.pangrova.parser.api.ast.statement.MaterializeFeatureViewStatement;
import org.pangrova.parser.api.ast.statement.PublishFeatureViewStatement;

import static org.assertj.core.api.Assertions.assertThat;

class FeatureViewParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void parsesVersionedCreatePublishAndMaterialize() {
        var create = (CreateFeatureViewStatement) parser.parse(
                "CREATE FEATURE VIEW IF NOT EXISTS lake.ml.customer_features "
                        + "VERSION 3 AS '{\"entity_keys\":[\"customer_id\"]}'");
        var publish = (PublishFeatureViewStatement) parser.parse(
                "ALTER FEATURE VIEW lake.ml.customer_features PUBLISH VERSION 3 "
                        + "EXPECTING EPOCH 2");
        var materialize = (MaterializeFeatureViewStatement) parser.parse(
                "MATERIALIZE FEATURE VIEW lake.ml.customer_features VERSION 3 "
                        + "SNAPSHOT '991'");

        assertThat(create.viewName().parts())
                .containsExactly("lake", "ml", "customer_features");
        assertThat(create.ifNotExists()).isTrue();
        assertThat(create.type()).isEqualTo(StatementType.CREATE_FEATURE_VIEW);
        assertThat(publish.expectedPublicationEpoch()).hasValue(2);
        assertThat(publish.type()).isEqualTo(StatementType.PUBLISH_FEATURE_VIEW);
        assertThat(materialize.sourceSnapshot()).isEqualTo("991");
        assertThat(materialize.type()).isEqualTo(StatementType.MATERIALIZE_FEATURE_VIEW);
    }

    @Test
    void printsRoundTripSql() {
        AstPrinter printer = new AstPrinter();
        for (String sql : java.util.List.of(
                "CREATE FEATURE VIEW f VERSION 1 AS '{\"entity_keys\":[\"id\"]}'",
                "ALTER FEATURE VIEW f PUBLISH VERSION 2 EXPECTING EPOCH 1",
                "MATERIALIZE FEATURE VIEW f VERSION 2 SNAPSHOT '42'")) {
            var statement = parser.parse(sql);
            assertThat(parser.parse(printer.print(statement))).isEqualTo(statement);
        }
    }
}
