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
package org.pangrova.coordinator.feature;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.pangrova.coordinator.feature.FeatureViewDefinitionValidator.ValidatedFeatureViewDefinition;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.common.util.DigestUtils;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.metadata.exception.MetastoreException;
import org.pangrova.metadata.store.MetadataJsonMapper;
import org.pangrova.metadata.feature.FeatureCatalogStore;
import org.pangrova.metadata.feature.FeatureCatalogStore.ExportRequest;
import org.pangrova.metadata.feature.FeatureCatalogStore.FeaturePublication;
import org.pangrova.metadata.feature.FeatureCatalogStore.FeatureViewId;
import org.pangrova.metadata.feature.FeatureCatalogStore.FeatureViewVersion;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.statement.CreateFeatureViewStatement;
import org.pangrova.parser.ast.statement.MaterializeFeatureViewStatement;
import org.pangrova.parser.ast.statement.PublishFeatureViewStatement;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

/** Governed Feature View catalog and durable export operations. */
public final class FeatureViewStatementService {

    private static final String DEFAULT_NAMESPACE = "default";

    private final FeatureCatalogStore store;
    private final DurableFeatureExportService exportService;
    private final AccessControl accessControl;
    private final FeatureViewDefinitionValidator validator;
    private final Clock clock;
    private final FeatureSourceCompiler sourceCompiler;

    public FeatureViewStatementService(
            FeatureCatalogStore store,
            DurableFeatureExportService exportService,
            AccessControl accessControl,
            FeatureSourceCompiler sourceCompiler) {
        this(store, exportService, accessControl, sourceCompiler,
                new FeatureViewDefinitionValidator(), Clock.systemUTC());
    }

    FeatureViewStatementService(
            FeatureCatalogStore store,
            DurableFeatureExportService exportService,
            AccessControl accessControl,
            FeatureSourceCompiler sourceCompiler,
            FeatureViewDefinitionValidator validator,
            Clock clock) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.exportService = Objects.requireNonNull(
                exportService, "exportService must not be null");
        this.accessControl = accessControl;
        this.sourceCompiler = Objects.requireNonNull(sourceCompiler, "sourceCompiler must not be null");
        this.validator = Objects.requireNonNull(validator, "validator must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /** Creates one immutable version and authorizes its exact source table. */
    public FeatureViewVersion create(
            CreateFeatureViewStatement statement, CoordinatorSessionContext session) {
        Actor actor = requireActor(session);
        FeatureViewId id = resolveId(statement.viewName(), actor);
        accessControl.checkCanCreateTable(
                actor.user(), id.catalogName(), id.schemaName(), id.viewName());
        var definition = validator.validate(statement.definitionJson());
        authorizeSource(actor.user(), definition.sourceTable());
        var compiled = sourceCompiler.compile(definition.sourceQuery(),
                sourceSession(session, definition.sourceTable()));
        validator.validateCompiledSource(definition, compiled);
        FeatureViewVersion candidate = new FeatureViewVersion(
                id, statement.version(), actor.user().principal(),
                definition.entityKeys(), definition.eventTimeColumn(),
                definition.sourceTable().toString(), definition.sourceQuery(),
                compiled.planBytes(), DigestUtils.sha256Hex(compiled.planBytes()),
                definition.transformVersion(), definition.schemaJson(),
                definition.freshnessTargetMillis(), definition.lineageJson(),
                definition.canonicalJson(), definition.sha256(), clock.instant());
        var existing = store.findFeatureView(id, statement.version());
        if (existing.isPresent()) {
            return acceptExisting(existing.orElseThrow(), candidate, statement.ifNotExists());
        }
        try {
            store.createFeatureView(candidate);
            return candidate;
        } catch (MetastoreException race) {
            if (statement.ifNotExists()) {
                var raced = store.findFeatureView(id, statement.version());
                if (raced.isPresent()) {
                    return acceptExisting(raced.orElseThrow(), candidate, true);
                }
            }
            throw race;
        }
    }

    /** Publishes an immutable version under the requested publication-epoch CAS fence. */
    public FeaturePublication publish(
            PublishFeatureViewStatement statement, CoordinatorSessionContext session) {
        Actor actor = requireActor(session);
        FeatureViewId id = resolveId(statement.viewName(), actor);
        accessControl.checkCanAlterTable(
                actor.user(), id.catalogName(), id.schemaName(), id.viewName());
        FeatureViewVersion version = requireVersion(id, statement.version());
        authorizeSource(actor.user(), source(version));
        return store.publishFeatureView(
                id, statement.version(), statement.expectedPublicationEpoch(),
                actor.user().principal(), clock.instant())
                .orElseThrow(() -> new PangrovaException(
                        PangrovaErrorCode.METADATA_STALE_EPOCH,
                        "Feature View publication epoch changed; refresh and retry"));
    }

    /** Submits one single-source immutable snapshot to the generic Native export state machine. */
    public ExportRequest materialize(
            MaterializeFeatureViewStatement statement,
            CoordinatorSessionContext session) {
        Actor actor = requireActor(session);
        FeatureViewId id = resolveId(statement.viewName(), actor);
        accessControl.checkCanAlterTable(
                actor.user(), id.catalogName(), id.schemaName(), id.viewName());
        FeatureViewVersion version = requireVersion(id, statement.version());
        authorizeSource(actor.user(), source(version));
        var definition = validator.validate(version.definitionJson());
        requirePersistedDefinition(definition, version);
        var compiled = sourceCompiler.compile(definition.sourceQuery(),
                sourceSession(session, definition.sourceTable()));
        validator.validateCompiledSource(definition, compiled);
        var source = source(version);
        return exportService.submit(
                version, statement.sourceSnapshot(),
                SessionContextConverter.toConnectorSession(session, source.catalog(), source.schema()),
                actor.user().principal(), compiled);
    }

    private static void requirePersistedDefinition(
            ValidatedFeatureViewDefinition definition, FeatureViewVersion version) {
        if (!definition.sha256().equals(version.definitionSha256())
                || !definition.sourceQuery().equals(version.sourceQuery())
                || !definition.sourceTable().toString().equals(version.sourceTable())
                || !definition.entityKeys().equals(version.entityKeys())
                || !definition.eventTimeColumn().equals(version.eventTimeColumn())
                || !definition.transformVersion().equals(version.transformVersion())
                || !sameOutputDefinition(definition, version)) {
            throw invalid("Persisted Feature View logical definition identity is inconsistent");
        }
    }

    private static boolean sameOutputDefinition(ValidatedFeatureViewDefinition definition, FeatureViewVersion version) {
        return sameJson(definition.schemaJson(), version.schemaJson())
                && definition.freshnessTargetMillis() == version.freshnessTargetMillis()
                && sameJson(definition.lineageJson(), version.lineageJson());
    }

    private FeatureViewVersion requireVersion(FeatureViewId id, long version) {
        return store.findFeatureView(id, version)
                .orElseThrow(() -> invalid("Feature View version does not exist"));
    }

    /** JSONB can reorder object keys; compare content, while retaining ordered arrays and exact values. */
    private static boolean sameJson(String expected, String actual) {
        try {
            return MetadataJsonMapper.instance().readTree(expected)
                    .equals(MetadataJsonMapper.instance().readTree(actual));
        } catch (JsonProcessingException failure) {
            throw invalid("Persisted Feature View logical definition contains invalid JSON");
        }
    }

    private static FeatureViewVersion acceptExisting(
            FeatureViewVersion existing,
            FeatureViewVersion candidate,
            boolean ifNotExists) {
        if (ifNotExists
                && existing.definitionSha256().equals(candidate.definitionSha256())) {
            return existing;
        }
        throw invalid("Feature View version already exists with different content");
    }

    private void authorizeSource(
            UserIdentity user,
            org.pangrova.spi.connector.metadata.table.QualifiedTableName source) {
        accessControl.checkCanSelectFromTable(
                user, source.catalog(), source.schema(), source.table());
    }

    private static org.pangrova.spi.connector.metadata.table.QualifiedTableName source(
            FeatureViewVersion version) {
        String[] parts = version.sourceTable().split("\\.", -1);
        if (parts.length != 3 || java.util.Arrays.stream(parts).anyMatch(String::isBlank)) {
            throw invalid("Persisted Feature View source identity is invalid");
        }
        return new org.pangrova.spi.connector.metadata.table.QualifiedTableName(
                parts[0], parts[1], parts[2]);
    }

    private static CoordinatorSessionContext sourceSession(CoordinatorSessionContext session,
            org.pangrova.spi.connector.metadata.table.QualifiedTableName source) {
        var properties = new java.util.HashMap<>(session.sessionProperties());
        properties.put("catalog", source.catalog());
        properties.put("schema", source.schema());
        return new CoordinatorSessionContext(session.queryId(), properties,
                session.userIdentity(), session.tenantId());
    }

    private Actor requireActor(CoordinatorSessionContext session) {
        requireAuthenticated(session);
        String catalog = session.sessionProperties().getOrDefault(
                "catalog", DEFAULT_NAMESPACE);
        String schema = session.sessionProperties().getOrDefault(
                "schema", DEFAULT_NAMESPACE);
        if (catalog.isBlank() || schema.isBlank()) {
            throw invalid("Session catalog and schema must not be blank");
        }
        return new Actor(session.userIdentity(), session.tenantId(), catalog, schema);
    }

    private void requireAuthenticated(CoordinatorSessionContext session) {
        if (accessControl == null || session == null || session.userIdentity() == null
                || session.userIdentity().principal() == null
                || session.userIdentity().principal().isBlank()
                || session.tenantId() == null || session.tenantId().isBlank()) {
            throw new PangrovaException(
                    PangrovaErrorCode.ACCESS_DENIED,
                    "Feature View operations require an authenticated tenant context");
        }
    }

    private static FeatureViewId resolveId(Identifier name, Actor actor) {
        List<String> parts = name.parts();
        return switch (parts.size()) {
            case 3 -> new FeatureViewId(
                    actor.tenantId(), parts.get(0), parts.get(1), parts.get(2));
            case 2 -> new FeatureViewId(
                    actor.tenantId(), actor.catalog(), parts.get(0), parts.get(1));
            case 1 -> new FeatureViewId(
                    actor.tenantId(), actor.catalog(), actor.schema(), parts.get(0));
            default -> throw invalid("Feature View name must have one to three parts");
        };
    }

    private static PangrovaException invalid(String message) {
        return new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }

    private record Actor(UserIdentity user, String tenantId, String catalog, String schema) {}
}
