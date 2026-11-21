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
import org.pangrova.parser.api.ast.statement.CreateSemanticViewStatement;
import org.pangrova.parser.api.ast.statement.PublishSemanticViewStatement;
import org.pangrova.parser.api.ast.statement.ShowSemanticViewsStatement;

import static org.assertj.core.api.Assertions.assertThat;

class SemanticViewParserTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void parsesImmutableCreateDefinition() {
        var statement = (CreateSemanticViewStatement) parser.parse(
                "CREATE SEMANTIC VIEW IF NOT EXISTS lake.sales.revenue VERSION 7 "
                        + "AS '{\"logicalTables\":[{\"name\":\"orders\","
                        + "\"source\":\"lake.sales.orders\"}],\"measures\":[{"
                        + "\"name\":\"revenue\",\"expression\":\"SUM(amount)\"}]}'");

        assertThat(statement.viewName().parts())
                .containsExactly("lake", "sales", "revenue");
        assertThat(statement.version()).isEqualTo(7);
        assertThat(statement.ifNotExists()).isTrue();
        assertThat(statement.definitionJson()).contains("logicalTables", "SUM(amount)");
        assertThat(statement.type()).isEqualTo(StatementType.CREATE_SEMANTIC_VIEW);
    }

    @Test
    void parsesFencedPublishAndScopedShow() {
        var publish = (PublishSemanticViewStatement) parser.parse(
                "ALTER SEMANTIC VIEW lake.sales.revenue PUBLISH VERSION 8 "
                        + "EXPECTING EPOCH 7");
        var show = (ShowSemanticViewsStatement) parser.parse(
                "SHOW SEMANTIC VIEWS FROM lake.sales");

        assertThat(publish.version()).isEqualTo(8);
        assertThat(publish.expectedPublicationEpoch()).hasValue(7);
        assertThat(publish.type()).isEqualTo(StatementType.PUBLISH_SEMANTIC_VIEW);
        assertThat(show.scope()).containsExactly("lake", "sales");
        assertThat(show.type()).isEqualTo(StatementType.SHOW_SEMANTIC_VIEWS);
    }

    @Test
    void printsRoundTripSqlWithoutLosingFenceOrDefinition() {
        String createSql = "CREATE SEMANTIC VIEW lake.sales.revenue VERSION 1 "
                + "AS '{\"logicalTables\":[{\"name\":\"orders\","
                + "\"source\":\"lake.sales.orders\"}],\"measures\":[{"
                + "\"name\":\"revenue\",\"expression\":\"SUM(amount)\"}]}'";
        var create = parser.parse(createSql);

        AstPrinter printer = new AstPrinter();
        assertThat(parser.parse(printer.print(create))).isEqualTo(create);
        var publish = parser.parse(
                "ALTER SEMANTIC VIEW lake.sales.revenue PUBLISH VERSION 2 "
                        + "EXPECTING EPOCH 1");
        assertThat(parser.parse(printer.print(publish))).isEqualTo(publish);
    }
}
