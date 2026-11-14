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
package org.pangrova.semantic.api.version;

import java.util.Objects;
import java.util.OptionalLong;

/** One immutable Semantic View version plus its tenant-scoped publication state. */
public record SemanticViewVersionStatus(
        SemanticViewVersion version,
        OptionalLong activeVersion,
        long publicationEpoch) {

    public SemanticViewVersionStatus {
        Objects.requireNonNull(version, "version must not be null");
        Objects.requireNonNull(activeVersion, "activeVersion must not be null");
        if (activeVersion.isPresent()) {
            if (activeVersion.getAsLong() <= 0 || publicationEpoch <= 0) {
                throw new IllegalArgumentException("published version and epoch must be positive");
            }
        } else if (publicationEpoch != 0) {
            throw new IllegalArgumentException("unpublished view must have epoch zero");
        }
    }

    public boolean active() {
        return activeVersion.isPresent()
                && activeVersion.getAsLong() == version.version();
    }
}
