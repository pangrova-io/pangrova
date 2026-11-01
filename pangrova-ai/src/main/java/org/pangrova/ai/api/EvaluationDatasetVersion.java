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
package org.pangrova.ai.api;

import java.time.Instant;
import java.util.Objects;

/** Immutable manifest and source snapshot for one evaluation dataset version. */
public record EvaluationDatasetVersion(
        EvaluationDatasetId id,
        long version,
        String ownerPrincipal,
        String sourceSnapshot,
        String manifestUri,
        String manifestSha256,
        long itemCount,
        String schemaJson,
        Instant createdAt) {

    public EvaluationDatasetVersion {
        Objects.requireNonNull(id, "id must not be null");
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        EvaluationValidation.requireText(ownerPrincipal, "ownerPrincipal");
        EvaluationValidation.requireText(sourceSnapshot, "sourceSnapshot");
        EvaluationValidation.requireText(manifestUri, "manifestUri");
        EvaluationValidation.requireSha256(manifestSha256, "manifestSha256");
        if (itemCount < 0) {
            throw new IllegalArgumentException("itemCount must not be negative");
        }
        EvaluationValidation.requireText(schemaJson, "schemaJson");
        Objects.requireNonNull(createdAt, "createdAt must not be null");
    }
}
