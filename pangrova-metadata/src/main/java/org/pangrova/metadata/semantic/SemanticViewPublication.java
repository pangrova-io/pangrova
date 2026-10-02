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
package org.pangrova.metadata.semantic;

import java.time.Instant;
import java.util.Objects;

/** Fenced active-version pointer for one Semantic View identity. */
public record SemanticViewPublication(
        SemanticViewId id,
        long activeVersion,
        long publicationEpoch,
        String publishedBy,
        Instant publishedAt) {

    public SemanticViewPublication {
        Objects.requireNonNull(id, "id must not be null");
        if (activeVersion <= 0 || publicationEpoch <= 0) {
            throw new IllegalArgumentException(
                    "activeVersion and publicationEpoch must be positive");
        }
        SemanticViewValidation.requireText(publishedBy, "publishedBy");
        Objects.requireNonNull(publishedAt, "publishedAt must not be null");
    }
}
