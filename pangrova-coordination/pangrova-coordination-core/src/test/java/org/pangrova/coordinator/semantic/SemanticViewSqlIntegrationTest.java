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
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext;
import org.pangrova.query.core.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.dispatch.task.semantic.CreateSemanticViewTask;
import org.pangrova.coordinator.query.execution.dispatch.task.semantic.PublishSemanticViewTask;
import org.pangrova.coordinator.query.execution.dispatch.task.semantic.ShowSemanticViewsTask;
import org.pangrova.query.core.statement.StatementClassifier;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.semantic.api.view.SemanticViewId;
import org.pangrova.semantic.api.view.SemanticViewStore;
import org.pangrova.persistence.semantic.postgresql.JdbcSemanticViewStore;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.CreateSemanticViewStatement;
import org.pangrova.parser.api.ast.statement.PublishSemanticViewStatement;
import org.pangrova.parser.api.ast.statement.ShowSemanticViewsStatement;
import org.pangrova.planner.core.analysis.SqlCompiler;
import org.pangrova.security.api.policy.Privilege;
import org.pangrova.security.api.policy.PrivilegeGrant;
import org.pangrova.security.policy.builtin.authorization.DynamicRbacAccessControl;
import org.pangrova.testing.testkit.security.policy.StaticGrantStore;
import org.pangrova.security.core.mask.ColumnMaskingProcessor;
import org.pangrova.testing.testkit.security.mask.EmptyColumnMaskStore;
import org.pangrova.testing.testkit.security.rowfilter.EmptyRowPolicyStore;
import org.pangrova.security.core.rowfilter.RowLevelSecurityFilter;
import org.pangrova.security.spi.authorization.AccessControl;
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
                .isInstanceOf(org.pangrova.security.api.error.AuthorizationDeniedException.class);
        assertThat(store.listVersions(VIEW)).isEmpty();

        var noAccessControl = new SemanticViewStatementService(store, null);
        var statement = new CreateSemanticViewStatement(
                Identifier.of("revenue"), 1, false, definition("gross_revenue"));
        assertThatThrownBy(() -> noAccessControl.create(statement, session))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
        var missingTenant = new QuerySession(
                "q-missing-tenant", session.sessionProperties(), USER, null, java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        assertThatThrownBy(() -> new SemanticViewStatementService(store, fullAccess())
                .create(statement, missingTenant))
                .isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                        .isEqualTo(PangrovaErrorCode.ACCESS_DENIED));
    }

    @Test
    void tenantAndSessionNamespaceAreEnforcedForShow() {
        StatementDispatcher dispatcher = dispatcher(fullAccess(), new AtomicReference<>());
        dispatcher.tryDispatch(null, createSql(1, false, "gross_revenue"),
                "q-create", session);

        var otherTenant = new QuerySession(
                "q-other", session.sessionProperties(), USER, "tenant-b", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());
        SemanticViewStatementService service = new SemanticViewStatementService(
                store, fullAccess());

        assertThat(service.show(new ShowSemanticViewsStatement(List.of()), otherTenant))
                .isEmpty();
        assertThat(service.show(
                new ShowSemanticViewsStatement(List.of("lake", "sales")), session))
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
        var service = new SemanticViewStatementService(store, accessControl);
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
