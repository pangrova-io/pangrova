package org.pangrova.coordinator.metadata;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.QueryStateMachine;
import org.pangrova.coordinator.query.statement.DdlExecutor;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.service.DefaultMetadataService;
import org.pangrova.metadata.service.MetadataSessionContext;
import org.pangrova.metadata.tag.JdbcColumnTagStore;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.testing.ContainerTestBase;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Parser, admitted query identity, migrated PG, plugin and real Paimon SDK; not HTTP/Native E2E. */
class DdlColumnTagRejectionSdkIntegrationTest extends ContainerTestBase {

    @TempDir Path warehouse;

    @Test
    void setAndUnsetColumnTagRejectBeforeAnyPhysicalOrMetadataMutation() throws Exception {
        try (var fixture = new ReadBindingSdkFixture(warehouse)) {
            FileStoreTable table = fixture.create("tag_reject");
            ReadBindingSdkFixture.write(table, 2);
            Identifier identifier = Identifier.create("db", "tag_reject");
            long snapshotBefore = table.snapshotManager().latestSnapshotId();
            String schemaBefore = table.rowType().toString();
            long rowsBefore = committedRows(table);
            assertThat(rowsBefore).isEqualTo(2);

            var metadataService = fixture.bootstrap.metadataService();
            DataSource tagAuthority = SharedPostgresContainer.isolatedDataSource("ddl_tag_rejection");
            ((DefaultMetadataService) metadataService).setColumnTagStore(new JdbcColumnTagStore(tagAuthority));
            var metadataSession = new MetadataSessionContext("reader", "bound", "db", "UTC", "en",
                    Map.of(), "tenant", "", fixture.context.queryId());
            var tableRef = new QualifiedTableRef("bound", "db", "tag_reject");
            metadataService.setTag(metadataSession, tableRef, "id", "pii_type", "PII_PHONE");
            Map<String, String> tagsBefore = metadataService.listTags(metadataSession, tableRef, "id");
            assertThat(tagsBefore).containsEntry("pii_type", "PII_PHONE");

            AtomicInteger invalidations = new AtomicInteger();
            var executor = new DdlExecutor(fixture.bootstrap.catalogRegistry(), null,
                    invalidations::incrementAndGet);
            var tracker = new CoordinatorTaskTracker();
            var state = new QueryStateMachine();
            var authority = state.bindConnectorAuthority(tracker.openQueryIssuance(fixture.context.queryId()));
            var admitted = fixture.context.withQueryAuthority(authority);
            assertThat(admitted.queryAuthority().orElseThrow()).isSameAs(authority);
            try {
                var compiler = new SqlCompiler();
                var set = (AlterTableStatement) compiler.parseToAst(
                        "ALTER TABLE bound.db.tag_reject ALTER COLUMN id SET TAG pii_type = 'PII_EMAIL'");
                var unset = (AlterTableStatement) compiler.parseToAst(
                        "ALTER TABLE bound.db.tag_reject ALTER COLUMN id UNSET TAG pii_type");
                assertThat(set).isInstanceOf(AlterTableStatement.SetColumnTag.class);
                assertThat(unset).isInstanceOf(AlterTableStatement.UnsetColumnTag.class);
                assertRejected(executor, set, admitted);
                assertRejected(executor, unset, admitted);
            } finally {
                state.tryTransition(QueryStateMachine.State.CANCELLED);
                tracker.sealQueryIssuance(fixture.context.queryId());
                authority.cleanupMetadata().requireSuccess();
                tracker.retireQueryIssuance(fixture.context.queryId());
            }

            assertThat(invalidations).hasValue(0);
            fixture.sdk.invalidateTable(identifier);
            FileStoreTable after = (FileStoreTable) fixture.sdk.getTable(identifier);
            assertThat(after.rowType().toString()).isEqualTo(schemaBefore);
            assertThat(after.snapshotManager().latestSnapshotId()).isEqualTo(snapshotBefore);
            assertThat(committedRows(after)).isEqualTo(rowsBefore);
            assertThat(metadataService.listTags(metadataSession, tableRef, "id")).isEqualTo(tagsBefore);
            assertThat(new JdbcColumnTagStore(tagAuthority).list("bound|db|tag_reject|id")).isEqualTo(tagsBefore);
        }
    }

    private static void assertRejected(DdlExecutor executor, AlterTableStatement statement,
                                       org.pangrova.coordinator.query.session.CoordinatorSessionContext admitted) {
        assertThatThrownBy(() -> executor.executeAlterTable(statement, admitted))
                .isInstanceOfSatisfying(PangrovaException.class, failure ->
                        assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE))
                .hasMessageContaining("durable tag storage and policy enforcement");
    }

    private static long committedRows(FileStoreTable table) {
        return table.newReadBuilder().newScan().plan().splits().stream()
                .map(DataSplit.class::cast)
                .mapToLong(DataSplit::rowCount)
                .sum();
    }
}
