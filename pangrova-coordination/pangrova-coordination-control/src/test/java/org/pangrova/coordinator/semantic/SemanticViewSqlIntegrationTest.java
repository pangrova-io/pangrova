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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext;
import org.pangrova.query.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.task.semantic.CreateSemanticViewTask;
import org.pangrova.coordinator.query.execution.dispatch.task.semantic.PublishSemanticViewTask;
import org.pangrova.coordinator.query.execution.dispatch.task.semantic.ShowSemanticViewsTask;
import org.pangrova.query.statement.StatementClassifier;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.semantic.api.identity.SemanticViewId;
import org.pangrova.semantic.version.SemanticViewService;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.semantic.api.version.SemanticViewStore;
import org.pangrova.persistence.semantic.postgresql.JdbcSemanticViewStore;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.CreateSemanticViewStatement;
import org.pangrova.parser.api.ast.statement.PublishSemanticViewStatement;
import org.pangrova.parser.api.ast.statement.ShowSemanticViewsStatement;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.policy.builtin.authorization.DynamicRbacAccessControl;
import org.pangrova.testing.testkit.security.policy.StaticGrantStore;
import org.pangrova.security.mask.ColumnMaskingProcessor;
import org.pangrova.testing.testkit.security.mask.EmptyColumnMaskStore;
import org.pangrova.testing.testkit.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.api.authorization.AccessControl;
import org.pangrova.security.api.identity.UserIdentity;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.pangrova.coordinator.testinfra.CoordinatorRoleFixtures.mappedRoles;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SemanticViewSqlIntegrationTest {

    private static final String TENANT = "tenant-a";
    private static final SemanticViewId VIEW =
            new SemanticViewId(TENANT, "lake", "sales", "revenue");
    private static final UserIdentity USER = new UserIdentity(
            "semantic-owner", Set.of("semantic-admin"), Map.of(), Instant.EPOCH);

    private SemanticViewStore store;
    private QuerySession session;

    @BeforeEach
    void setUp() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("semantic_sql");
        store = new JdbcSemanticViewStore(dataSource);
        session = new QuerySession(
                "semantic-query", Map.of("catalog", "lake", "schema", "sales"),
                USER, TENANT, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
    }

    @Test
    void sqlEntryCreatesPublishesAndShowsTypedRowsThroughDispatcher() {
        AtomicReference<StatementResult> completed = new AtomicReference<>();
        StatementDispatcher dispatcher = dispatcher(fullAccess(), completed);

        dispatcher.tryDispatch(null, createSql(1, false, "gross_revenue"),
                "q-create", session);
        dispatcher.tryDispatch(null, createSql(1, true, "gross_revenue"),
                "q-create-idempotent", session);
        dispatcher.tryDispatch(null,
                "ALTER SEMANTIC VIEW revenue PUBLISH VERSION 1",
                "q-publish-initial", session);
        dispatcher.tryDispatch(null, createSql(2, false, "net_revenue"),
                "q-create-v2", session);
        dispatcher.tryDispatch(null,
                "ALTER SEMANTIC VIEW revenue PUBLISH VERSION 2 EXPECTING EPOCH 1",
                "q-publish-v2", session);
        dispatcher.tryDispatch(null, "SHOW SEMANTIC VIEWS FROM lake.sales",
                "q-show", session);

        assertThat(store.listVersions(VIEW)).hasSize(2);
        assertThat(store.findPublication(VIEW).orElseThrow())
                .satisfies(publication -> {
                    assertThat(publication.activeVersion()).isEqualTo(2);
                    assertThat(publication.publicationEpoch()).isEqualTo(2);
                });
        assertThat(completed.get().columnNames()).containsExactly(
                "catalog_name", "schema_name", "view_name", "version", "active",
                "active_version", "publication_epoch", "validation_state", "owner", "created_at");
        assertThat(completed.get().rows()).hasSize(2);
        assertThat(completed.get().rows().get(0).getString("view_name"))
                .isEqualTo("revenue");
        assertThat(completed.get().rows().get(0).getBoolean("active")).isTrue();
        assertThat(completed.get().rows().get(0).getLong("active_version")).isEqualTo(2);
    }

    @Test
    void ifNotExistsRejectsDifferentContentAndPublishRejectsStaleFence() {
        StatementDispatcher dispatcher = dispatcher(fullAccess(), new AtomicReference<>());
        dispatcher.tryDispatch(null, createSql(1, false, "gross_revenue"),
                "q-create", session);
        dispatcher.tryDispatch(null,
                "ALTER SEMANTIC VIEW revenue PUBLISH VERSION 1",
                "q-publish", session);
        dispatcher.tryDispatch(null, createSql(2, false, "net_revenue"),
                "q-create-v2", session);
        dispatcher.tryDispatch(null,
                "ALTER SEMANTIC VIEW revenue PUBLISH VERSION 2 EXPECTING EPOCH 1",
                "q-publish-v2", session);
        dispatcher.tryDispatch(null,
                "ALTER SEMANTIC VIEW revenue PUBLISH VERSION 1 EXPECTING EPOCH 2",
                "q-publish-v1-again", session);

        assertThatThrownBy(() -> dispatcher.tryDispatch(
                null, createSql(1, true, "changed"), "q-conflict", session))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE));
        assertThatThrownBy(() -> dispatcher.tryDispatch(
                null,
                "ALTER SEMANTIC VIEW revenue PUBLISH VERSION 2 EXPECTING EPOCH 1",
                "q-stale", session))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.METADATA_STALE_EPOCH));
    }

    @Test
    void authorizationIsFailClosedBeforeAnyMetadataIsPublished() {
        AccessControl createOnly = rbac(List.of(
                PrivilegeGrant.of("semantic-admin", Privilege.CREATE_TABLE,
                        "TABLE", "lake", "sales", "revenue", null)));
        StatementDispatcher deniedDispatcher = dispatcher(createOnly, new AtomicReference<>());

        assertThatThrownBy(() -> deniedDispatcher.tryDispatch(
                null, createSql(1, false, "gross_revenue"), "q-denied", session))
                .isInstanceOf(org.pangrova.security.api.exception.AuthorizationDeniedException.class);
        assertThat(store.listVersions(VIEW)).isEmpty();

        var noAccessControl = new SemanticViewService(store, null);
        var statement = new CreateSemanticViewStatement(
                Identifier.of("revenue"), 1, false, definition("gross_revenue"));
        assertThatThrownBy(() -> new CreateSemanticViewTask(noAccessControl)
                .execute(statement, new StatementExecutionContext(null, "q-no-access-control", session)))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
        var missingTenant = new QuerySession(
                "q-missing-tenant", session.sessionProperties(), USER, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        assertThatThrownBy(() -> new CreateSemanticViewTask(new SemanticViewService(store, fullAccess()))
                .execute(statement, new StatementExecutionContext(null, "q-missing-tenant", missingTenant)))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void publicationRechecksCurrentSourceAccessBeforeAdvancingEpoch() {
        var authorized = dispatcher(fullAccess(), new AtomicReference<>());
        authorized.tryDispatch(null, createSql(1, false, "gross_revenue"), "q-v1", session);
        authorized.tryDispatch(null, "ALTER SEMANTIC VIEW revenue PUBLISH VERSION 1", "q-publish", session);
        authorized.tryDispatch(null, createSql(2, false, "net_revenue"), "q-v2", session);

        AccessControl alterViewOnly = rbac(List.of(
                PrivilegeGrant.of("semantic-admin", Privilege.ALTER,
                        "TABLE", "lake", "sales", "revenue", null)));
        var denied = dispatcher(alterViewOnly, new AtomicReference<>());
        assertThatThrownBy(() -> denied.tryDispatch(null,
                "ALTER SEMANTIC VIEW revenue PUBLISH VERSION 2 EXPECTING EPOCH 1", "q-denied", session))
                .isInstanceOf(org.pangrova.security.api.exception.AuthorizationDeniedException.class);
        var unchanged = store.findPublication(VIEW).orElseThrow();
        assertThat(unchanged.activeVersion()).isEqualTo(1);
        assertThat(unchanged.publicationEpoch()).isEqualTo(1);
    }

    @Test
    void tenantAndSessionNamespaceAreEnforcedForShow() {
        StatementDispatcher dispatcher = dispatcher(fullAccess(), new AtomicReference<>());
        dispatcher.tryDispatch(null, createSql(1, false, "gross_revenue"),
                "q-create", session);

        var otherTenant = new QuerySession(
                "q-other", session.sessionProperties(), USER, "tenant-b", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        SemanticViewService service = new SemanticViewService(
                store, fullAccess());

        var show = new ShowSemanticViewsTask(service);
        assertThat(show.execute(new ShowSemanticViewsStatement(List.of()),
                new StatementExecutionContext(null, "q-other", otherTenant)).rows())
                .isEmpty();
        assertThat(show.execute(new ShowSemanticViewsStatement(List.of("lake", "sales")),
                new StatementExecutionContext(null, "q-visible", session)).rows())
                .hasSize(1);
    }

    @Test
    void semanticViewMutationIsLeaderRoutedAndAudited() {
        SqlCompiler compiler = new SqlCompiler();
        Statement create = compiler.parseToAst(createSql(1, false, "gross_revenue"));
        Statement publish = compiler.parseToAst(
                "ALTER SEMANTIC VIEW revenue PUBLISH VERSION 1");
        Statement show = compiler.parseToAst("SHOW SEMANTIC VIEWS");

        assertThat(StatementClassifier.requiresLeaderRouting(create)).isTrue();
        assertThat(StatementClassifier.requiresLeaderRouting(publish)).isTrue();
        assertThat(StatementClassifier.requiresDdlAudit(create)).isTrue();
        assertThat(StatementClassifier.requiresDdlAudit(publish)).isTrue();
        assertThat(StatementClassifier.requiresLeaderRouting(show)).isFalse();
        assertThat(StatementClassifier.isShow(show)).isTrue();
    }

    private StatementDispatcher dispatcher(
            AccessControl accessControl,
            AtomicReference<StatementResult> completed) {
        var service = new SemanticViewService(store, accessControl);
        var tasks = new HashMap<Class<? extends Statement>, MetadataTask<?>>();
        tasks.put(CreateSemanticViewStatement.class, new CreateSemanticViewTask(service));
        tasks.put(PublishSemanticViewStatement.class, new PublishSemanticViewTask(service));
        tasks.put(ShowSemanticViewsStatement.class, new ShowSemanticViewsTask(service));
        var factory = new MetadataExecutionFactory(tasks);
        var context = new StatementDispatcherContext(
                new SqlCompiler(), null, List.of(factory),
                List.of(event -> completed.set(event.result())), accessControl);
        return new StatementDispatcher(context);
    }

    private static AccessControl fullAccess() {
        return rbac(List.of(PrivilegeGrant.of(
                "semantic-admin", Privilege.ALL, "SCHEMA",
                "lake", "sales", null, null)));
    }

    private static AccessControl rbac(List<PrivilegeGrant> grants) {
        return new DynamicRbacAccessControl(
                new StaticGrantStore(grants),
                mappedRoles(java.util.Map.ofEntries(java.util.Map.entry("semantic-admin", "semantic-admin"))),
                new RowLevelSecurityFilter(new EmptyRowPolicyStore()),
                new ColumnMaskingProcessor(new EmptyColumnMaskStore()));
    }

    private static String createSql(long version, boolean ifNotExists, String measure) {
        return "CREATE SEMANTIC VIEW " + (ifNotExists ? "IF NOT EXISTS " : "")
                + "revenue VERSION " + version + " AS '" + definition(measure) + "'";
    }

    private static String definition(String measure) {
        return "{\"logicalTables\":[{\"name\":\"orders\","
                + "\"source\":\"lake.sales.orders\"}],\"measures\":[{"
                + "\"name\":\"" + measure + "\",\"expression\":\"SUM(amount)\"}]}";
    }
}
