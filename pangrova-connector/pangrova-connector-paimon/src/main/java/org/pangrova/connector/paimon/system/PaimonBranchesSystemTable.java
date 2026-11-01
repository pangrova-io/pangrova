package org.pangrova.connector.paimon.system;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.spi.connector.data.value.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.BranchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes Paimon branches via {@code table$branches}.
 *
 * <p>Each row represents a named branch.</p>
 */
public final class PaimonBranchesSystemTable implements ConnectorSystemTable {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonBranchesSystemTable.class);

    private static final List<String> COLUMNS = List.of(
            "branch_name", "created_from_snapshot");

    private final Table table;

    public PaimonBranchesSystemTable(Table table) {
        this.table = table;
    }

    @Override
    public List<String> columns() {
        return List.copyOf(COLUMNS);
    }

    @Override
    public List<TypedRow> rows() {
        if (!(table instanceof FileStoreTable fst)) {
            return List.of();
        }
        BranchManager branchManager = fst.branchManager();
        List<TypedRow> rows = new ArrayList<>();
        try {
            for (String branchName : branchManager.branches()) {
                TypedRow row = TypedRow.builder()
                        .putString("branch_name", branchName)
                        .putRaw("created_from_snapshot", null)
                        .build();
                rows.add(row);
            }
        } catch (Exception e) {
            LOG.warn("event=branches_system_table_failed error={}", e.getMessage());
        }
        return List.copyOf(rows);
    }
}
