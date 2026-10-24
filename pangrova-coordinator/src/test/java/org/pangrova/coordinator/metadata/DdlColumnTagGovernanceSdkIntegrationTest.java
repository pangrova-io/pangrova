package org.pangrova.coordinator.metadata;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.governance.classification.ColumnTagService;
import org.pangrova.governance.classification.InMemoryAuditConfigStore;
import org.pangrova.governance.classification.TagBasedSecurityPolicy;
import org.pangrova.metadata.control.GovernanceBindingType;
import org.pangrova.metadata.control.InMemoryTableControlGovernanceBindingStore;
import org.pangrova.metadata.control.TableControlPolicyBinder;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.service.DefaultMetadataService;
import org.pangrova.metadata.service.MetadataSessionContext;
import org.pangrova.metadata.tag.JdbcColumnTagStore;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.security.store.PersistentColumnMaskStore;
import org.pangrova.spi.security.AccessDeniedException;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
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
            ReadBindingSdkFixture.write(table, 2);
            Identifier identifier = Identifier.create("db", "tag_governed");
            long snapshotBefore = table.snapshotManager().latestSnapshotId();
            String schemaBefore = table.rowType().toString();
            long rowsBefore = committedRows(table);
            assertThat(rowsBefore).isEqualTo(2);

            var metadataService = (DefaultMetadataService) fixture.bootstrap.metadataService();
            DataSource tagAuthority = SharedPostgresContainer.isolatedDataSource("ddl_tag_governance");
            DataSource maskAuthority = SharedPostgresContainer.isolatedDataSource("ddl_tag_masks");
            metadataService.setColumnTagStore(new JdbcColumnTagStore(tagAuthority));
            ColumnTagService tagService = new ColumnTagService();
            metadataService.setColumnTagService(tagService);
            PersistentColumnMaskStore maskStore = new PersistentColumnMaskStore(maskAuthority);
            maskStore.initSchema();
            tagService.addListener(new TagBasedSecurityPolicy(
                    maskStore, new InMemoryAuditConfigStore()));

            InMemoryTableControlGovernanceBindingStore bindingStore =
                    new InMemoryTableControlGovernanceBindingStore();
            var binder = new TableControlPolicyBinder(
                    fixture.bootstrap.catalogRegistry().tableControl(), bindingStore);
            AtomicInteger invalidations = new AtomicInteger();
            var executor = new DdlExecutor(fixture.bootstrap.catalogRegistry(), null,
                    invalidations::incrementAndGet);
            executor.setMetadataService(metadataService);
            executor.setPolicyBinder(binder);

            var tracker = new CoordinatorTaskTracker();
            var state = new QueryStateMachine();
            var authority = state.bindConnectorAuthority(tracker.openQueryIssuance(fixture.context.queryId()));
            var admitted = fixture.context.withQueryAuthority(authority);
            try {
                var compiler = new SqlCompiler();
                var adopt = (AlterTableStatement) compiler.parseToAst(
                        "ALTER TABLE bound.db.tag_governed ADOPT");
                var set = (AlterTableStatement) compiler.parseToAst(
                        "ALTER TABLE bound.db.tag_governed ALTER COLUMN id SET TAG pii_type = 'PII_EMAIL'");
                var unset = (AlterTableStatement) compiler.parseToAst(
                        "ALTER TABLE bound.db.tag_governed ALTER COLUMN id UNSET TAG pii_type");
                assertThat(set).isInstanceOf(AlterTableStatement.SetColumnTag.class);
                assertThat(unset).isInstanceOf(AlterTableStatement.UnsetColumnTag.class);

                assertThatThrownBy(() -> executor.executeAlterTable(set, admitted))
                        .isInstanceOf(AccessDeniedException.class)
                        .hasMessageContaining("OBSERVED");
                assertThat(invalidations).hasValue(0);
                assertThat(new JdbcColumnTagStore(tagAuthority)
                        .list("bound|db|tag_governed|id")).isEmpty();

                executor.executeAlterTable(adopt, admitted);
                String tableId = fixture.bootstrap.catalogRegistry().tableControl()
                        .find("bound", "db", "tag_governed").orElseThrow().tableId().toString();

                executor.executeAlterTable(set, admitted);

                var metadataSession = metadataSession(fixture);
                var tableRef = new QualifiedTableRef("bound", "db", "tag_governed");
                assertThat(metadataService.listTags(metadataSession, tableRef, "id"))
                        .containsEntry("pii_type", "PII_EMAIL");
                assertThat(new JdbcColumnTagStore(tagAuthority)
                        .list("bound|db|tag_governed|id"))
                        .containsEntry("pii_type", "PII_EMAIL");
                assertThat(maskStore.getColumnMasks("bound", "db", "tag_governed", "id"))
                        .singleElement()
                        .satisfies(mask -> {
                            assertThat(mask.maskName()).isEqualTo("auto_pii_id");
                            assertThat(mask.maskExpression()).isEqualTo("mask_email");
                        });
                assertThat(bindingStore.list(UUID.fromString(tableId)))
                        .singleElement()
                        .satisfies(binding -> {
                            assertThat(binding.bindingType())
                                    .isEqualTo(GovernanceBindingType.COLUMN_TAG);
                            assertThat(binding.bindingRef()).isEqualTo("id:pii_type");
                            assertThat(binding.config()).containsEntry("value", "PII_EMAIL");
                        });

                executor.executeAlterTable(unset, admitted);

                assertThat(metadataService.listTags(metadataSession, tableRef, "id")).isEmpty();
                assertThat(new JdbcColumnTagStore(tagAuthority)
                        .list("bound|db|tag_governed|id")).isEmpty();
                assertThat(maskStore.getColumnMasks("bound", "db", "tag_governed", "id")).isEmpty();
                assertThat(bindingStore.list(UUID.fromString(tableId))).isEmpty();
                assertThat(invalidations).hasValue(2);
            } finally {
                state.tryTransition(QueryStateMachine.State.CANCELLED);
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
