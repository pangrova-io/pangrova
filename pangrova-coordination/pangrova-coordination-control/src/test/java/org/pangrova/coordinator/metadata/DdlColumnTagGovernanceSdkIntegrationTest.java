package org.pangrova.coordinator.metadata;


import org.pangrova.query.api.enums.QueryState;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.table.api.enums.GovernanceBindingType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.session.MetadataSessionContext;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.planner.analysis.SqlCompiler;
import org.pangrova.persistence.security.postgresql.mask.JdbcColumnMaskStore;
import org.pangrova.security.api.exception.AccessDeniedException;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Parser, admitted query identity, migrated PG, plugin and real Paimon SDK; not HTTP/Native E2E. */
class DdlColumnTagGovernanceSdkIntegrationTest extends ContainerTestBase {

    @TempDir Path warehouse;

    @Test
    void tagRequiresAdoptionAndThenPersistsTagMaskAndBinding() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            FileStoreTable table = fixture.create("tag_governed");
            fixture.sdk.alterTable(Identifier.create("db", "tag_governed"), java.util.List.of(
                    org.apache.paimon.schema.SchemaChange.addColumn("email", org.apache.paimon.types.DataTypes.STRING())),
                    false);
            table = (FileStoreTable) fixture.sdk.getTable(Identifier.create("db", "tag_governed"));
            var writeBuilder = table.newBatchWriteBuilder();
            try (var write = writeBuilder.newWrite(); var commit = writeBuilder.newCommit()) {
                write.write(org.apache.paimon.data.GenericRow.of(1L,
                        org.apache.paimon.data.BinaryString.fromString("alice@example.com")));
                write.write(org.apache.paimon.data.GenericRow.of(2L, null));
                commit.commit(write.prepareCommit());
            }
            Identifier identifier = Identifier.create("db", "tag_governed");
            long snapshotBefore = table.snapshotManager().latestSnapshotId();
            String schemaBefore = table.rowType().toString();
            long rowsBefore = committedRows(table);
            assertThat(rowsBefore).isEqualTo(2);

            var metadataService = fixture.bootstrap.metadataService();
            var maskStore = new JdbcColumnMaskStore(fixture.dataSource);
            var boundMasks = new org.pangrova.metadata.security.TableControlColumnMaskStore(
                    fixture.bootstrap.catalogRegistry().tableGovernance(), maskStore);
            var bindingStore = new org.pangrova.persistence.table.postgresql.policy
                    .PostgresTablePolicyRepository(fixture.dataSource);
            AtomicInteger invalidations = new AtomicInteger();
            var executor = new DdlExecutor(fixture.bootstrap.catalogRegistry(), null,
                    invalidations::incrementAndGet);
            executor.setMetadataService(metadataService);

            var tracker = new CoordinatorTaskTracker();
            var state = new QueryStateMachine();
            var authority = state.bindConnectorAuthority(tracker.openQueryIssuance(fixture.context.queryId()));
            var admitted = fixture.context.withQueryAuthority(authority);
            try {
                var compiler = new SqlCompiler();
                var adopt = (AlterTableStatement) compiler.parseToAst(
                        "ALTER TABLE bound.db.tag_governed ADOPT");
                var set = (AlterTableStatement) compiler.parseToAst(
                        "ALTER TABLE bound.db.tag_governed ALTER COLUMN email SET TAG pii_type = 'PII_EMAIL'");
                var unset = (AlterTableStatement) compiler.parseToAst(
                        "ALTER TABLE bound.db.tag_governed ALTER COLUMN email UNSET TAG pii_type");
                assertThat(set).isInstanceOf(AlterTableStatement.SetColumnTag.class);
                assertThat(unset).isInstanceOf(AlterTableStatement.UnsetColumnTag.class);

                assertThatThrownBy(() -> executor.executeAlterTable(set, admitted))
                        .isInstanceOf(AccessDeniedException.class)
                        .hasMessageContaining("OBSERVED");
                assertThat(invalidations).hasValue(0);
                try (var connection = fixture.dataSource.getConnection();
                     var statement = connection.createStatement();
                     var rows = statement.executeQuery("SELECT count(*) FROM table_control_governance_binding")) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getLong(1)).isZero();
                }

                executor.executeAlterTable(adopt, admitted);
                String tableId = fixture.bootstrap.catalogRegistry().tableControl()
                        .find("bound", "db", "tag_governed").orElseThrow().tableId().toString();

                executor.executeAlterTable(set, admitted);

                var metadataSession = metadataSession(fixture);
                var tableRef = new QualifiedTableRef("bound", "db", "tag_governed");
                assertThat(metadataService.listTags(metadataSession, tableRef, "email"))
                        .containsEntry("pii_type", "PII_EMAIL");
                assertThat(bindingStore.list(UUID.fromString(tableId))).singleElement()
                        .satisfies(binding -> assertThat(binding.config()).containsEntry("value", "PII_EMAIL"));
                assertThat(boundMasks.getColumnMasks("bound", "db", "tag_governed", "email"))
                        .singleElement()
                        .satisfies(mask -> {
                            assertThat(mask.maskName()).isEqualTo("auto_pii_email");
                            assertThat(mask.maskExpression()).isEqualTo("CASE WHEN {column} IS NULL THEN NULL ELSE '***@***' END");
                        });
                assertThat(bindingStore.list(UUID.fromString(tableId)))
                        .singleElement()
                        .satisfies(binding -> {
                            assertThat(binding.bindingType())
                                    .isEqualTo(GovernanceBindingType.COLUMN_TAG);
                            assertThat(binding.bindingRef()).isEqualTo("5:emailpii_type");
                            assertThat(binding.config()).containsEntry("value", "PII_EMAIL");
                        });

                executor.executeAlterTable(unset, admitted);

                assertThat(metadataService.listTags(metadataSession, tableRef, "email")).isEmpty();
                assertThat(bindingStore.list(UUID.fromString(tableId))).isEmpty();
                assertThat(boundMasks.getColumnMasks("bound", "db", "tag_governed", "email")).isEmpty();
                assertThat(bindingStore.list(UUID.fromString(tableId))).isEmpty();
                assertThat(invalidations).hasValue(2);
            } finally {
                state.tryTransition(QueryState.CANCELLED);
                tracker.sealQueryIssuance(fixture.context.queryId());
                authority.cleanupMetadata().requireSuccess();
                tracker.retireQueryIssuance(fixture.context.queryId());
            }

            fixture.sdk.invalidateTable(identifier);
            FileStoreTable after = (FileStoreTable) fixture.sdk.getTable(identifier);
            assertThat(after.rowType().toString()).isEqualTo(schemaBefore);
            assertThat(after.snapshotManager().latestSnapshotId()).isEqualTo(snapshotBefore);
            assertThat(committedRows(after)).isEqualTo(rowsBefore);
        }
    }

    private static MetadataSessionContext metadataSession(ReadBindingSdkFixture fixture) {
        return new MetadataSessionContext("reader", "bound", "db", "UTC", "en",
                Map.of(), "tenant", "", fixture.context.queryId());
    }

    private static long committedRows(FileStoreTable table) {
        return table.newReadBuilder().newScan().plan().splits().stream()
                .map(DataSplit.class::cast)
                .mapToLong(DataSplit::rowCount)
                .sum();
    }
}
