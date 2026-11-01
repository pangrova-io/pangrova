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
package org.pangrova.semantic.api;

import java.time.Instant;
import java.util.Objects;

/** Immutable, validated definition version of a governed Semantic View. */
public record SemanticViewVersion(
        SemanticViewId id,
        long version,
        String ownerPrincipal,
        String definitionJson,
        String definitionSha256,
        SemanticViewValidationState validationState,
        String validationMessage,
        Instant createdAt) {

    public SemanticViewVersion {
        Objects.requireNonNull(id, "id must not be null");
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        SemanticViewValidation.requireText(ownerPrincipal, "ownerPrincipal");
        SemanticViewValidation.requireText(definitionJson, "definitionJson");
        SemanticViewValidation.requireSha256(definitionSha256, "definitionSha256");
        Objects.requireNonNull(validationState, "validationState must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
        if (validationState == SemanticViewValidationState.VALIDATED) {
            if (validationMessage != null) {
                throw new IllegalArgumentException(
                        "validated Semantic View must not have a validationMessage");
            }
        } else {
            SemanticViewValidation.requireText(validationMessage, "validationMessage");
        }
    }
}
