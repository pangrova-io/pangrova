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
package org.pangrova.coordinator.semantic;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.CoordinatorSessionContext;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.semantic.SemanticViewId;
import org.pangrova.metadata.semantic.SemanticViewPublication;
import org.pangrova.metadata.semantic.SemanticViewStore;
import org.pangrova.metadata.semantic.SemanticViewValidationState;
import org.pangrova.metadata.semantic.SemanticViewVersion;
import org.pangrova.metadata.semantic.SemanticViewVersionStatus;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.statement.CreateSemanticViewStatement;
import org.pangrova.parser.ast.statement.PublishSemanticViewStatement;
import org.pangrova.parser.ast.statement.ShowSemanticViewsStatement;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

/** Governed Semantic View metadata operations; this service never reads table data. */
public final class SemanticViewStatementService {

    private static final String DEFAULT_NAMESPACE = "default";
    private static final int MAX_SHOW_ROWS = 10_000;

    private final SemanticViewStore store;
    private final AccessControl accessControl;
    private final SemanticViewDefinitionValidator definitionValidator;
    private final Clock clock;

    public SemanticViewStatementService(
            SemanticViewStore store,
            AccessControl accessControl) {
        this(store, accessControl, new SemanticViewDefinitionValidator(), Clock.systemUTC());
    }

    SemanticViewStatementService(
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

    /** Creates one immutable version after schema validation and source-table authorization. */
    public SemanticViewVersion create(
            CreateSemanticViewStatement statement,
            CoordinatorSessionContext session) {
        Actor actor = requireActor(session);
        SemanticViewId id = resolveViewId(statement.viewName(), actor);
        accessControl.checkCanCreateTable(
                actor.user(), id.catalogName(), id.schemaName(), id.viewName());
        var validated = definitionValidator.validate(statement.definitionJson());
        authorizeSources(actor.user(), validated.sources());
        SemanticViewVersion candidate = new SemanticViewVersion(
                id, statement.version(), actor.user().principal(),
                validated.canonicalJson(), validated.sha256(),
                SemanticViewValidationState.VALIDATED, null, clock.instant());
        return createIdempotently(candidate, statement.ifNotExists());
    }

    /** Publishes a validated version under a publication-epoch compare-and-set fence. */
    public SemanticViewPublication publish(
            PublishSemanticViewStatement statement,
            CoordinatorSessionContext session) {
        Actor actor = requireActor(session);
        SemanticViewId id = resolveViewId(statement.viewName(), actor);
        accessControl.checkCanAlterTable(
                actor.user(), id.catalogName(), id.schemaName(), id.viewName());
        SemanticViewVersion version = store.findVersion(id, statement.version())
                .orElseThrow(() -> invalid("Semantic View version does not exist"));
        var validated = definitionValidator.validate(version.definitionJson());
        if (!validated.sha256().equals(version.definitionSha256())) {
            throw invalid("Semantic View definition checksum is invalid");
        }
        authorizeSources(actor.user(), validated.sources());
        return store.publishVersion(
                id, statement.version(), statement.expectedPublicationEpoch(),
                actor.user().principal(), clock.instant())
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.METADATA_STALE_EPOCH,
                        "Semantic View publication epoch changed; refresh and retry"));
    }

    /** Lists version status within the authenticated tenant and authorized schema. */
    public List<SemanticViewVersionStatus> show(
            ShowSemanticViewsStatement statement,
            CoordinatorSessionContext session) {
        Actor actor = requireActor(session);
        Scope scope = resolveScope(statement.scope(), actor);
        accessControl.checkCanAccessSchema(actor.user(), scope.catalog(), scope.schema());
        accessControl.checkCanShowTables(actor.user(), scope.catalog(), scope.schema());
        List<SemanticViewVersionStatus> statuses = store.listVersionStatuses(
                actor.tenantId(), scope.catalog(), scope.schema(), MAX_SHOW_ROWS + 1);
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
        } catch (MetastoreException race) {
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

    private Actor requireActor(CoordinatorSessionContext session) {
        if (accessControl == null || session == null || session.userIdentity() == null
                || session.userIdentity().principal() == null
                || session.userIdentity().principal().isBlank()
                || session.tenantId() == null || session.tenantId().isBlank()) {
            throw new PangrovaException(
                    PangrovaErrorCode.ACCESS_DENIED,
                    "Semantic View operations require an authenticated tenant context");
        }
        String catalog = session.sessionProperties().getOrDefault(
                "catalog", DEFAULT_NAMESPACE);
        String schema = session.sessionProperties().getOrDefault(
                "schema", DEFAULT_NAMESPACE);
        if (catalog.isBlank() || schema.isBlank()) {
            throw invalid("Session catalog and schema must not be blank");
        }
        return new Actor(session.userIdentity(), session.tenantId(), catalog, schema);
    }

    private static SemanticViewId resolveViewId(Identifier name, Actor actor) {
        List<String> parts = name.parts();
        return switch (parts.size()) {
            case 3 -> new SemanticViewId(
                    actor.tenantId(), parts.get(0), parts.get(1), parts.get(2));
            case 2 -> new SemanticViewId(
                    actor.tenantId(), actor.catalog(), parts.get(0), parts.get(1));
            case 1 -> new SemanticViewId(
                    actor.tenantId(), actor.catalog(), actor.schema(), parts.get(0));
            default -> throw invalid("Semantic View name must have one to three parts");
        };
    }

    private static Scope resolveScope(List<String> parts, Actor actor) {
        return switch (parts.size()) {
            case 2 -> new Scope(parts.get(0), parts.get(1));
            case 1 -> new Scope(actor.catalog(), parts.get(0));
            case 0 -> new Scope(actor.catalog(), actor.schema());
            default -> throw invalid("SHOW SEMANTIC VIEWS scope must have zero to two parts");
        };
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }

    private record Actor(UserIdentity user, String tenantId, String catalog, String schema) {}

    private record Scope(String catalog, String schema) {}
}
