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
package org.pangrova.parser.ast.statement;

import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

import java.util.Objects;

/** Creates one immutable, explicitly versioned Feature View definition. */
public record CreateFeatureViewStatement(
        Identifier viewName,
        long version,
        boolean ifNotExists,
        String definitionJson) implements Statement {

    public CreateFeatureViewStatement {
        Objects.requireNonNull(viewName, "viewName must not be null");
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        if (definitionJson == null || definitionJson.isBlank()) {
            throw new IllegalArgumentException("definitionJson must not be blank");
        }
    }

    @Override
    public StatementType type() {
        return StatementType.CREATE_FEATURE_VIEW;
    }
}
