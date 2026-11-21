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

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.semantic.api.identity.SemanticViewId;
import org.pangrova.semantic.api.publication.SemanticViewPublication;

import java.util.List;
import java.util.OptionalLong;

/** Authorized operations on exact, tenant-scoped semantic view identities. */
@Experimental
public interface SemanticViewOperations {

    /** Creates an immutable version. An idempotent replay must have the same validated definition. */
    SemanticViewVersion create(
            UserIdentity user, SemanticViewId id, long version, String definitionJson, boolean ifNotExists);

    /**
     * Rechecks source privileges and requires the exact current epoch. An empty expectation requires no
     * publication.
     */
    SemanticViewPublication publish(
            UserIdentity user, SemanticViewId id, long version, OptionalLong expectedPublicationEpoch);

    /** Lists version status after checking schema access for the authenticated tenant. */
    List<SemanticViewVersionStatus> show(UserIdentity user, String tenantId, String catalog, String schema);
}
