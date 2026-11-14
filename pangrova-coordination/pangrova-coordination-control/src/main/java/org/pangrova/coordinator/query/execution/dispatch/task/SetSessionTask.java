package org.pangrova.coordinator.query.execution.dispatch.task;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.parser.api.ast.SetSessionStatement;
import org.pangrova.query.dispatch.StatementExecutionContext;
import org.pangrova.query.dispatch.StatementResult;

/** Reports that generic server-side session assignments have no execution capability. */
public final class SetSessionTask implements MetadataTask<SetSessionStatement> {
    @Override
    public StatementResult execute(SetSessionStatement statement, StatementExecutionContext context) {
        throw new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                "Server-side SET is unsupported. Supply supported session properties in the query request.");
    }
}
