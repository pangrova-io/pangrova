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
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Durable metadata contract for immutable Semantic View versions and their active pointer. */
public interface SemanticViewStore {

    void createVersion(SemanticViewVersion version);

    Optional<SemanticViewVersion> findVersion(SemanticViewId id, long version);

    List<SemanticViewVersion> listVersions(SemanticViewId id);

    List<SemanticViewVersionStatus> listVersionStatuses(
            String tenantId, String catalogName, String schemaName, int limit);

    Optional<SemanticViewVersion> findActiveVersion(SemanticViewId id);

    Optional<SemanticViewPublication> findPublication(SemanticViewId id);

    /**
     * Publishes a validated version under compare-and-set semantics.
     * An empty expectation means the caller requires no current publication.
     * Returns the exact committed publication, or empty when the fence did not match.
     */
    Optional<SemanticViewPublication> publishVersion(
            SemanticViewId id,
            long version,
            OptionalLong expectedPublicationEpoch,
            String publishedBy,
            Instant publishedAt);
}
