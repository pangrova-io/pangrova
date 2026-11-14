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
package org.pangrova.semantic.version;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.semantic.api.exception.SemanticStoreException;
import org.pangrova.semantic.api.identity.SemanticViewId;
import org.pangrova.semantic.api.publication.SemanticViewPublication;
import org.pangrova.semantic.api.enums.SemanticViewValidationState;
import org.pangrova.semantic.api.version.SemanticViewOperations;
import org.pangrova.semantic.api.version.SemanticViewStore;
import org.pangrova.semantic.api.version.SemanticViewVersion;
import org.pangrova.semantic.api.version.SemanticViewVersionStatus;
import org.pangrova.semantic.validation.SemanticViewDefinitionValidator;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/** Governed Semantic View metadata operations; this service never reads table data. */
public final class SemanticViewService implements SemanticViewOperations {

    private static final int MAX_SHOW_ROWS = 10_000;

    private final SemanticViewStore store;
    private final AccessControl accessControl;
    private final SemanticViewDefinitionValidator definitionValidator;
    private final Clock clock;

    public SemanticViewService(
            SemanticViewStore store,
            AccessControl accessControl) {
        this(store, accessControl, new SemanticViewDefinitionValidator(), Clock.systemUTC());
    }

    SemanticViewService(
            SemanticViewStore store,
            AccessControl accessControl,
            SemanticViewDefinitionValidator definitionValidator,
            Clock clock) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.accessControl = accessControl;
        this.definitionValidator = Objects.requireNonNull(
                definitionValidator, "definitionValidator must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public SemanticViewVersion create(
            UserIdentity user, SemanticViewId id, long version, String definitionJson, boolean ifNotExists) {
        requireActor(user, id.tenantId());
        accessControl.checkCanCreateTable(
                user, id.catalogName(), id.schemaName(), id.viewName());
        var validated = definitionValidator.validate(definitionJson);
        authorizeSources(user, validated.sources());
        SemanticViewVersion candidate = new SemanticViewVersion(
                id, version, user.principal(),
                validated.canonicalJson(), validated.sha256(),
                SemanticViewValidationState.VALIDATED, null, clock.instant());
        return createIdempotently(candidate, ifNotExists);
    }

    @Override
    public SemanticViewPublication publish(
            UserIdentity user, SemanticViewId id, long version, OptionalLong expectedPublicationEpoch) {
        requireActor(user, id.tenantId());
        accessControl.checkCanAlterTable(
                user, id.catalogName(), id.schemaName(), id.viewName());
        SemanticViewVersion stored = store.findVersion(id, version)
                .orElseThrow(() -> invalid("Semantic View version does not exist"));
        var validated = definitionValidator.validate(stored.definitionJson());
        if (!validated.sha256().equals(stored.definitionSha256())) {
            throw invalid("Semantic View definition checksum is invalid");
        }
        authorizeSources(user, validated.sources());
        return store.publishVersion(
                id, version, expectedPublicationEpoch,
                user.principal(), clock.instant())
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.METADATA_STALE_EPOCH,
                        "Semantic View publication epoch changed; refresh and retry"));
    }

    @Override
    public List<SemanticViewVersionStatus> show(
            UserIdentity user, String tenantId, String catalog, String schema) {
        requireActor(user, tenantId);
        accessControl.checkCanAccessSchema(user, catalog, schema);
        accessControl.checkCanShowTables(user, catalog, schema);
        List<SemanticViewVersionStatus> statuses = store.listVersionStatuses(
                tenantId, catalog, schema, MAX_SHOW_ROWS + 1);
        if (statuses.size() > MAX_SHOW_ROWS) {
            throw new PangrovaException(
                    PangrovaErrorCode.COORDINATOR_MEMORY_EXCEEDED,
                    "SHOW SEMANTIC VIEWS exceeds the 10000-row control-plane limit");
        }
        return statuses;
    }

    private SemanticViewVersion createIdempotently(
            SemanticViewVersion candidate, boolean ifNotExists) {
        var existing = store.findVersion(candidate.id(), candidate.version());
        if (existing.isPresent()) {
            return acceptExisting(existing.orElseThrow(), candidate, ifNotExists);
        }
        try {
            store.createVersion(candidate);
            return candidate;
        } catch (SemanticStoreException race) {
            if (ifNotExists) {
                var raced = store.findVersion(candidate.id(), candidate.version());
                if (raced.isPresent()) {
                    return acceptExisting(raced.orElseThrow(), candidate, true);
                }
            }
            throw race;
        }
    }

    private static SemanticViewVersion acceptExisting(
            SemanticViewVersion existing,
            SemanticViewVersion candidate,
            boolean ifNotExists) {
        if (ifNotExists && existing.definitionSha256().equals(candidate.definitionSha256())) {
            return existing;
        }
        throw invalid("Semantic View version already exists with different content");
    }

    private void authorizeSources(UserIdentity user, List<QualifiedTableName> sources) {
        for (QualifiedTableName source : sources) {
            accessControl.checkCanSelectFromTable(
                    user, source.catalog(), source.schema(), source.table());
        }
    }

    private void requireActor(UserIdentity user, String tenantId) {
        if (accessControl == null || user == null || user.principal() == null || user.principal().isBlank()
                || tenantId == null || tenantId.isBlank()) {
            throw new PangrovaException(
                    PangrovaErrorCode.ACCESS_DENIED,
                    "Semantic View operations require an authenticated tenant context");
        }
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }
}
