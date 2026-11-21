package org.pangrova.parser.format;

import org.pangrova.parser.api.ast.statement.CreateFeatureViewStatement;
import org.pangrova.parser.api.ast.statement.CreateSemanticViewStatement;
import org.pangrova.parser.api.ast.statement.MaterializeFeatureViewStatement;
import org.pangrova.parser.api.ast.statement.PublishFeatureViewStatement;
import org.pangrova.parser.api.ast.statement.PublishSemanticViewStatement;
import org.pangrova.parser.api.ast.statement.ShowSemanticViewsStatement;

import java.util.stream.Collectors;

/** SQL text for semantic and feature view statements. */
final class SemanticFeatureStatementPrinter {

    private final AstPrinter printer;

    SemanticFeatureStatementPrinter(AstPrinter printer) {
        this.printer = printer;
    }

    String printCreateSemanticView(CreateSemanticViewStatement statement) {
        return "CREATE SEMANTIC VIEW "
                + (statement.ifNotExists() ? "IF NOT EXISTS " : "")
                + printer.printId(statement.viewName())
                + " VERSION " + statement.version()
                + " AS '" + statement.definitionJson().replace("'", "''") + "'";
    }

    String printPublishSemanticView(PublishSemanticViewStatement statement) {
        String sql = "ALTER SEMANTIC VIEW " + printer.printId(statement.viewName())
                + " PUBLISH VERSION " + statement.version();
        return statement.expectedPublicationEpoch().isPresent()
                ? sql + " EXPECTING EPOCH "
                        + statement.expectedPublicationEpoch().getAsLong()
                : sql;
    }

    String printShowSemanticViews(ShowSemanticViewsStatement statement) {
        if (statement.scope().isEmpty()) {
            return "SHOW SEMANTIC VIEWS";
        }
        return "SHOW SEMANTIC VIEWS FROM "
                + statement.scope().stream()
                        .map(printer::quote)
                        .collect(Collectors.joining("."));
    }

    String printCreateFeatureView(CreateFeatureViewStatement statement) {
        return "CREATE FEATURE VIEW "
                + (statement.ifNotExists() ? "IF NOT EXISTS " : "")
                + printer.printId(statement.viewName())
                + " VERSION " + statement.version()
                + " AS '" + statement.definitionJson().replace("'", "''") + "'";
    }

    String printPublishFeatureView(PublishFeatureViewStatement statement) {
        String sql = "ALTER FEATURE VIEW " + printer.printId(statement.viewName())
                + " PUBLISH VERSION " + statement.version();
        return statement.expectedPublicationEpoch().isPresent()
                ? sql + " EXPECTING EPOCH "
                        + statement.expectedPublicationEpoch().getAsLong()
                : sql;
    }

    String printMaterializeFeatureView(MaterializeFeatureViewStatement statement) {
        return "MATERIALIZE FEATURE VIEW " + printer.printId(statement.viewName())
                + " VERSION " + statement.version()
                + " SNAPSHOT '" + statement.sourceSnapshot().replace("'", "''") + "'";
    }
}
