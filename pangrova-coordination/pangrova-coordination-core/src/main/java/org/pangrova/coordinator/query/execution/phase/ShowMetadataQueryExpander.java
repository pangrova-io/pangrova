package org.pangrova.coordinator.query.execution.phase;

import java.util.List;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.ShowBranchesStatement;
import org.pangrova.parser.ast.statement.ShowMetadataStatement;
import org.pangrova.parser.ast.statement.ShowTagsStatement;
import org.pangrova.planner.MetadataTableQuery;
import org.pangrova.connector.spi.metadata.MetadataTableKind;

/** Expands SHOW into a typed query over the connector's declared Native metadata source. */
final class ShowMetadataQueryExpander {
    @FunctionalInterface
    interface ListingResolver {
        MetadataTableQuery resolve(List<String> tableName, MetadataTableKind kind);
    }

    private ShowMetadataQueryExpander() {}

    static MetadataTableQuery expand(Statement statement, ListingResolver resolver) {
        if (statement instanceof ShowTagsStatement tags) {
            return resolver.resolve(tags.tableName().parts(), MetadataTableKind.TAGS);
        }
        if (statement instanceof ShowBranchesStatement branches) {
            return resolver.resolve(branches.tableName().parts(), MetadataTableKind.BRANCHES);
        }
        if (!(statement instanceof ShowMetadataStatement show)) return null;
        MetadataTableKind kind;
        if (show instanceof ShowMetadataStatement.ShowSnapshots) {
            kind = MetadataTableKind.SNAPSHOTS;
        } else if (show instanceof ShowMetadataStatement.ShowFiles) {
            kind = MetadataTableKind.FILES;
        } else if (show instanceof ShowMetadataStatement.ShowManifests) {
            kind = MetadataTableKind.MANIFESTS;
        } else if (show instanceof ShowMetadataStatement.ShowPartitions) {
            kind = MetadataTableKind.PARTITIONS;
        } else {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "SHOW metadata kind has no Native system-table reader: " + show.getClass().getSimpleName());
        }
        return resolver.resolve(show.tableName(), kind);
    }
}
