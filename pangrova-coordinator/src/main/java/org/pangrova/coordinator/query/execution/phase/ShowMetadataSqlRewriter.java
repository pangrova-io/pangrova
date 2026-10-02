package org.pangrova.coordinator.query.execution.phase;

import java.util.List;
import java.util.regex.Pattern;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.ShowMetadataStatement;

/** Routes SHOW metadata through the ordinary authorized distributed scan. */
final class ShowMetadataSqlRewriter {

    private static final Pattern SIMPLE_NAME = Pattern.compile("[a-z_][a-z0-9_]*");

    private ShowMetadataSqlRewriter() {}

    static String rewrite(Statement statement) {
        if (!(statement instanceof ShowMetadataStatement show)) {
            return null;
        }
        if (!(show instanceof ShowMetadataStatement.ShowSnapshots)) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "SHOW metadata kind has no Native system-table reader: "
                            + show.getClass().getSimpleName());
        }
        List<String> parts = show.tableName();
        if (parts.isEmpty() || parts.stream().anyMatch(
                part -> !SIMPLE_NAME.matcher(part).matches())) {
            throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "SHOW SNAPSHOTS requires an unquoted lowercase table name until typed relation identity is available");
        }
        return "SELECT * FROM " + String.join(".", parts) + "$snapshots";
    }
}
