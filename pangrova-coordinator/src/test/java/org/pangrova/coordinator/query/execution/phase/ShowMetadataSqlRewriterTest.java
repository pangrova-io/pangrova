package org.pangrova.coordinator.query.execution.phase;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.parser.ast.statement.ShowMetadataStatement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShowMetadataSqlRewriterTest {

    @Test
    void showSnapshotsUsesAuthorizedSelectPath() {
        assertThat(ShowMetadataSqlRewriter.rewrite(
                new ShowMetadataStatement.ShowSnapshots(List.of("lake", "db", "orders"))))
                .isEqualTo("SELECT * FROM lake.db.orders$snapshots");
    }

    @Test
    void showFilesAndManifestsUseAuthorizedNativeScans() {
        assertThat(ShowMetadataSqlRewriter.rewrite(
                new ShowMetadataStatement.ShowFiles(List.of("db", "orders"))))
                .isEqualTo("SELECT * FROM db.orders$files");
        assertThat(ShowMetadataSqlRewriter.rewrite(
                new ShowMetadataStatement.ShowManifests(List.of("db", "orders"))))
                .isEqualTo("SELECT * FROM db.orders$manifests");
    }

    @Test
    void unsupportedKindsFailClosedWithoutCoordinatorRows() {
        assertThatThrownBy(() -> ShowMetadataSqlRewriter.rewrite(
                new ShowMetadataStatement.ShowPartitions(List.of("db", "orders"))))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("no Native system-table reader");
    }

    @Test
    void mixedCaseAsciiNameRetainsItsIdentity() {
        assertThat(ShowMetadataSqlRewriter.rewrite(
                new ShowMetadataStatement.ShowSnapshots(List.of("db", "Orders"))))
                .isEqualTo("SELECT * FROM db.Orders$snapshots");
    }

    @Test
    void namesThatCannotBeLosslesslyRewrittenFailClosed() {
        assertThatThrownBy(() -> ShowMetadataSqlRewriter.rewrite(
                new ShowMetadataStatement.ShowSnapshots(List.of("db", "orders;drop"))))
                .isInstanceOf(PangrovaException.class);
    }
}
