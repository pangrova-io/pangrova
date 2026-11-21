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
package org.pangrova.parser.api.ast.statement;

import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

import java.util.Objects;

/** Submits one immutable Feature View snapshot to the generic Native export pipeline. */
public record MaterializeFeatureViewStatement(
        Identifier viewName,
        long version,
        String sourceSnapshot) implements Statement {

    public MaterializeFeatureViewStatement {
        Objects.requireNonNull(viewName, "viewName must not be null");
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        if (sourceSnapshot == null || sourceSnapshot.isBlank()) {
            throw new IllegalArgumentException("sourceSnapshot must not be blank");
        }
    }

    @Override
    public StatementType type() {
        return StatementType.MATERIALIZE_FEATURE_VIEW;
    }
}
