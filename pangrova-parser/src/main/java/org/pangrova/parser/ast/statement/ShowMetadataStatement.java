package org.pangrova.parser.ast.statement;

import java.util.List;

import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.StatementType;

/**
 * Represents SHOW SNAPSHOTS/FILES/MANIFESTS/PARTITIONS FROM/OF table.
 * Each variant maps to a connector system table query.
 */
public sealed interface ShowMetadataStatement extends Statement {

    List<String> tableName();

    /** SHOW SNAPSHOTS FROM/OF table. */
    record ShowSnapshots(List<String> tableName) implements ShowMetadataStatement {
        public ShowSnapshots {
            tableName = List.copyOf(tableName);
        }
    }

    /** SHOW FILES FROM/OF table. */
    record ShowFiles(List<String> tableName) implements ShowMetadataStatement {
        public ShowFiles {
            tableName = List.copyOf(tableName);
        }
    }

    /** SHOW MANIFESTS FROM/OF table. */
    record ShowManifests(List<String> tableName) implements ShowMetadataStatement {
        public ShowManifests {
            tableName = List.copyOf(tableName);
        }
    }

    /** SHOW PARTITIONS FROM/OF table. */
    record ShowPartitions(List<String> tableName) implements ShowMetadataStatement {
        public ShowPartitions {
            tableName = List.copyOf(tableName);
        }
    }

    @Override
    default StatementType type() {
        return StatementType.SHOW_METADATA;
    }
}
