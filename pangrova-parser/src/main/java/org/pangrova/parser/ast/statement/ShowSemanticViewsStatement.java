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

import org.pangrova.parser.ast.Statement;

import java.util.List;

/** Lists tenant-visible Semantic View versions in one catalog and schema. */
public record ShowSemanticViewsStatement(List<String> scope) implements Statement {

    public ShowSemanticViewsStatement {
        scope = scope == null ? List.of() : List.copyOf(scope);
        if (scope.size() > 2) {
            throw new IllegalArgumentException(
                    "SHOW SEMANTIC VIEWS scope must be schema or catalog.schema");
        }
        if (scope.stream().anyMatch(part -> part == null || part.isBlank())) {
            throw new IllegalArgumentException(
                    "SHOW SEMANTIC VIEWS scope parts must not be blank");
        }
    }
}
