package org.pangrova.coordinator.governance;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.pangrova.governance.quality.rule.GovernanceQueryException;
import org.pangrova.governance.quality.rule.GovernanceQueryRunner;
import org.pangrova.governance.quality.rule.QualityQueryResult;
import org.pangrova.governance.quality.rule.QualityReadRequest;
import org.pangrova.parser.ast.ExpressionItem;
import org.pangrova.parser.ast.Query;
import org.pangrova.planner.parse.AstToSqlNodeConverter;

/** Constructs quality SELECTs from a parsed scalar expression and exact identifier components. */
public final class QualityReadSql {
    private static final String SOURCE = "__pg_quality_source__";
    private static final String PREDICATE = "__pg_quality_predicate__";
    private static final PostgresqlSqlDialect DIALECT = new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT) {
        @Override
        public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
            unparseFetchUsingLimit(writer, offset, fetch);
        }
    };
    private static final SqlParser.Config PARSER = SqlParser.config()
            .withUnquotedCasing(Casing.UNCHANGED).withQuotedCasing(Casing.UNCHANGED);
    private static final org.pangrova.parser.SqlParser EXPRESSION_PARSER = new org.pangrova.parser.SqlParser();

    private QualityReadSql() { }

    /** Parses a single expression; Statement escapes and unbound parameters cannot escape the read shape. */
    public static String render(QualityReadRequest request) {
        Objects.requireNonNull(request, "request");
        try {
            SqlNode predicate = parsePredicate(request.expression());
            predicate.accept(new ScalarPredicate());
            String template = request.type() == GovernanceQueryRunner.QueryType.COUNTS
                    ? "SELECT COUNT(*) AS total_rows, COUNT(CASE WHEN (__pg_quality_predicate__) IS NOT TRUE "
                        + "THEN 1 END) AS failed_rows FROM __pg_quality_source__"
                    : "SELECT * FROM __pg_quality_source__ WHERE (__pg_quality_predicate__) IS NOT TRUE LIMIT "
                        + QualityQueryResult.MAX_SAMPLES;
            SqlNode query = SqlParser.create(template, PARSER).parseQuery().accept(new SqlShuttle() {
                @Override
                public SqlNode visit(SqlIdentifier identifier) {
                    if (identifier.isSimple() && identifier.getSimple().equals(PREDICATE)) return predicate;
                    if (identifier.isSimple() && identifier.getSimple().equals(SOURCE)) {
                        return new SqlIdentifier(List.of(request.catalog(), request.schema(), request.table()),
                                SqlParserPos.ZERO);
                    }
                    return super.visit(identifier);
                }
            });
            return query.toSqlString(DIALECT).getSql();
        } catch (SqlParseException | org.pangrova.parser.ParseException failure) {
            throw new GovernanceQueryException(request.expression(),
                    "Quality rule requires one SQL expression", failure);
        }
    }

    private static SqlNode parsePredicate(String expression) {
        // Use the production grammar. Only extract an expression after verifying that
        // user text has not added any clause to the enclosing SELECT.
        var statement = EXPRESSION_PARSER.parse("SELECT (" + expression + ")");
        if (!(statement instanceof Query query) || query.distinct() || query.selectItems().size() != 1
                || !(query.selectItems().get(0) instanceof ExpressionItem item) || item.alias().isPresent()
                || !query.from().isEmpty() || !query.joins().isEmpty() || query.where().isPresent()
                || !query.groupBy().isEmpty() || query.having().isPresent() || query.qualify().isPresent()
                || !query.windowDefinitions().isEmpty() || !query.orderBy().isEmpty() || query.rerankBy().isPresent()
                || query.limit().isPresent() || query.offset().isPresent()) {
            throw new GovernanceQueryException(expression, "Quality rule requires one SQL expression");
        }
        try {
            return new AstToSqlNodeConverter().convertExpression(item.expression());
        } catch (IllegalArgumentException failure) {
            throw new GovernanceQueryException(expression,
                    "Quality predicate cannot be compiled: " + failure.getMessage(), failure);
        }
    }

    private static final class ScalarPredicate extends SqlBasicVisitor<Void> {
        @Override
        public Void visit(SqlDynamicParam parameter) {
            throw new GovernanceQueryException("?", "Persisted quality expressions cannot contain unbound parameters");
        }
    }
}
