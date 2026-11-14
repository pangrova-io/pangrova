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
package org.pangrova.planner.analysis;

import org.pangrova.planner.enums.PlannedDmlOperation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;

/**
 * Identifies the write target of a DML operation extracted from the compiled plan.
 */
public record DmlWriteTarget(
        List<String> qualifiedTableName,
        PlannedDmlOperation operation,
        Optional<WriteDeclarationSnapshot> declaration
) {
    public DmlWriteTarget {
        qualifiedTableName = List.copyOf(qualifiedTableName);
        Objects.requireNonNull(declaration, "declaration");
        if (operation == null) {
            throw new NullPointerException("operation must not be null");
        }
    }

}
