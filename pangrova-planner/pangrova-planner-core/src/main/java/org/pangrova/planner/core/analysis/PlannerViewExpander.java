package org.pangrova.planner.core.analysis;

import org.pangrova.planner.api.error.PlannerException;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.planner.core.parse.PangrovaParserFactory;
import org.pangrova.planner.core.validation.PangrovaSqlOperatorTable;

/** One conversion's view expansion, retaining its cluster, catalog bindings and parser semantics. */
final class PlannerViewExpander implements RelOptTable.ViewExpander {
    private static final int MAX_DEPTH = 64;
    private final PangrovaParserFactory parser;
    private final CalciteCatalogReader catalog;
    private final RelOptCluster cluster;
    private final PangrovaSqlOperatorTable operators;
    private final Deque<ViewIdentity> active = new ArrayDeque<>();

    PlannerViewExpander(PangrovaParserFactory parser, CalciteCatalogReader catalog,
            RelOptCluster cluster, PangrovaSqlOperatorTable operators) {
        this.parser = parser;
        this.catalog = catalog;
        this.cluster = cluster;
        this.operators = operators;
    }

    static SqlToRelConverter.Config converterConfig() {
        return SqlToRelConverter.config().addRelBuilderConfigTransform(config -> config.withBloat(-1));
    }

    @Override
    public RelRoot expandView(RelDataType rowType, String sql, List<String> schemaPath, List<String> viewPath) {
        var identity = new ViewIdentity(List.copyOf(viewPath == null ? schemaPath : viewPath),
                viewPath == null ? sql : "");
        if (active.contains(identity) || active.size() >= MAX_DEPTH) {
            throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                    "Recursive or excessively nested view expansion at " + identity.path());
        }
        active.push(identity);
        try {
            var query = parser.parseToSqlNode(sql);
            if (!query.isA(SqlKind.QUERY)) {
                throw new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                        "A view definition must be a read-only query");
            }
            var scopedCatalog = catalog.withSchemaPath(schemaPath);
            var validator = PlannerComponentFactory.buildValidator(operators, scopedCatalog, cluster.getTypeFactory());
            var validated = validator.validate(query);
            var converter = new SqlToRelConverter(this, validator, scopedCatalog, cluster,
                    StandardConvertletTable.INSTANCE, converterConfig());
            return converter.convertQuery(validated, false, false);
        } finally {
            active.pop();
        }
    }

    private record ViewIdentity(List<String> path, String anonymousSql) {}
}
