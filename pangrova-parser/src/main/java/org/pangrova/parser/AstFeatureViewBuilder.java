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
package org.pangrova.parser;

import org.antlr.v4.runtime.Token;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.parser.ast.statement.CreateFeatureViewStatement;
import org.pangrova.parser.ast.statement.MaterializeFeatureViewStatement;
import org.pangrova.parser.ast.statement.PublishFeatureViewStatement;

import java.util.OptionalLong;

/** Builds governed Feature View metadata and export statements. */
final class AstFeatureViewBuilder extends PangrovaSqlBaseVisitor<Object> {

    private final AstBuilder parent;

    AstFeatureViewBuilder(AstBuilder parent) {
        this.parent = parent;
    }

    @Override
    public CreateFeatureViewStatement visitCreateFeatureView(
            PangrovaSqlParser.CreateFeatureViewContext context) {
        return new CreateFeatureViewStatement(
                parent.visitQualifiedName(context.qualifiedName()),
                parsePositiveLong(context.version, "Feature View version"),
                context.EXISTS() != null,
                AstDdlDmlBuilder.stripQuotes(context.definition.getText()));
    }

    @Override
    public PublishFeatureViewStatement visitPublishFeatureView(
            PangrovaSqlParser.PublishFeatureViewContext context) {
        OptionalLong expected = context.expectedEpoch == null
                ? OptionalLong.empty()
                : OptionalLong.of(parsePositiveLong(
                        context.expectedEpoch, "expected Feature View publication epoch"));
        return new PublishFeatureViewStatement(
                parent.visitQualifiedName(context.qualifiedName()),
                parsePositiveLong(context.version, "Feature View version"), expected);
    }

    @Override
    public MaterializeFeatureViewStatement visitMaterializeFeatureView(
            PangrovaSqlParser.MaterializeFeatureViewContext context) {
        return new MaterializeFeatureViewStatement(
                parent.visitQualifiedName(context.qualifiedName()),
                parsePositiveLong(context.version, "Feature View version"),
                AstDdlDmlBuilder.stripQuotes(context.snapshot.getText()));
    }

    private static long parsePositiveLong(Token token, String label) {
        try {
            long value = Long.parseLong(token.getText());
            if (value <= 0) {
                throw new NumberFormatException("non-positive");
            }
            return value;
        } catch (NumberFormatException failure) {
            throw new ParseException(
                    PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    label + " must be a positive 64-bit integer",
                    token.getLine(), token.getCharPositionInLine());
        }
    }
}
