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

import java.util.Objects;
import java.util.OptionalLong;

/** Publishes a validated Semantic View version using a monotonic publication-epoch fence. */
public record PublishSemanticViewStatement(
        Identifier viewName,
        long version,
        OptionalLong expectedPublicationEpoch) implements Statement {

    public PublishSemanticViewStatement {
        Objects.requireNonNull(viewName, "viewName must not be null");
        Objects.requireNonNull(
                expectedPublicationEpoch, "expectedPublicationEpoch must not be null");
        if (version <= 0 || (expectedPublicationEpoch.isPresent()
                && expectedPublicationEpoch.getAsLong() <= 0)) {
            throw new IllegalArgumentException("version and publication epoch must be positive");
        }
    }
}
