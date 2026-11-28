package org.pangrova.planner.analysis;

import org.pangrova.planner.api.exception.PlannerException;
import org.pangrova.planner.metadata.PlanTable;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.planner.sqlnode.PangrovaParserFactory;
import org.pangrova.planner.validation.PangrovaSqlOperatorTable;
import org.pangrova.connector.api.metadata.table.ConnectorDerivedTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorReadBinding;
import org.pangrova.planner.metadata.catalog.MetadataResolvedTable;

/** Expands frozen connector definitions before policy application, without consulting a live catalog. */
final class DerivedQueryExpansion extends RelShuttleImpl {
    private static final int MAX_DEPTH = 64;
    private final PangrovaParserFactory parser;
    private final CalciteCatalogReader outerCatalog;
    private final PangrovaSqlOperatorTable operators;
    private final Deque<Identity> active = new ArrayDeque<>();

    private DerivedQueryExpansion(PangrovaParserFactory parser, CalciteCatalogReader catalog,
            PangrovaSqlOperatorTable operators) {
        this.parser = parser;
        this.outerCatalog = catalog;
        this.operators = operators;
    }

    static RelNode expand(RelNode relation, PangrovaParserFactory parser,
            CalciteCatalogReader catalog, PangrovaSqlOperatorTable operators) {
        return relation.accept(new DerivedQueryExpansion(parser, catalog, operators));
    }

    @Override
    public RelNode visit(TableScan scan) {
        var table = PlanTable.require(scan);
        var physical = table.unwrap(MetadataResolvedTable.class);
        // Compilation can inspect the selected shape. A derived relation is
        // expanded away, so it must still verify its owner before expansion.
        if (physical != null && physical.selectedReadBinding().map(ConnectorReadBinding::handle)
                .filter(ConnectorDerivedTableHandle.class::isInstance).isEmpty()) {
            return scan;
        }
        var binding = table.unwrap(ConnectorReadBinding.class);
        if (binding == null || !(binding.handle() instanceof ConnectorDerivedTableHandle derived)) return scan;
        binding.declaration().requireExactDeclaration();
        var identity = new Identity(List.copyOf(table.getQualifiedName()), binding.sourceIdentity());
        if (active.contains(identity) || active.size() >= MAX_DEPTH) {
            throw invalid("Recursive or excessively nested connector derived query: " + identity.name());
        }
        active.push(identity);
        try {
            return expand(scan, derived).accept(this);
        } finally {
            active.pop();
        }
    }

    private RelNode expand(TableScan scan, ConnectorDerivedTableHandle derived) {
        var query = derived.derivedQuery();
        var parsed = parser.parseToSqlNode(query.sql());
        if (!parsed.isA(SqlKind.QUERY)) throw invalid("Connector derived definition must be a read-only query");
        var cluster = scan.getCluster();
        var root = CalciteSchema.createRootSchema(false, false);
        var name = PlanTable.require(scan).getQualifiedName();
        var catalogPath = name.subList(0, Math.max(0, name.size() - 2));
        for (var input : query.inputs()) {
            root.add(input.alias(), new DerivedQueryInputTable(input.binding(), catalogPath));
        }
        // This catalog contains only the explicitly supplied immutable inputs.
        // Names in a definition cannot resolve to an ambient/live catalog relation.
        var catalog = new CalciteCatalogReader(root, List.of(), cluster.getTypeFactory(), outerCatalog.getConfig());
        var validator = PlannerComponentFactory.buildValidator(operators, catalog, cluster.getTypeFactory());
        var validated = validator.validate(parsed);
        var converter = new SqlToRelConverter(
                new PlannerViewExpander(parser, catalog, cluster, operators), validator, catalog, cluster,
                StandardConvertletTable.INSTANCE, PlannerViewExpander.converterConfig());
        RelNode expanded = converter.convertQuery(validated, false, false).project();
        expanded = PlannerDecorrelator.safeDecorrelate(expanded);
        expanded = SubqueryRelationalExpansion.expand(expanded);
        if (!containsFrozenInput(expanded)) {
            throw invalid("Connector derived query must retain a frozen source for authorization");
        }
        var expected = scan.getRowType();
        var actual = expanded.getRowType();
        if (!actual.getFieldNames().equals(expected.getFieldNames())
                || !SqlTypeUtil.equalAsStructSansNullability(cluster.getTypeFactory(), actual, expected,
                        org.apache.calcite.sql.validate.SqlNameMatchers.withCaseSensitive(true))) {
            throw invalid("Connector derived output differs from its frozen declaration: " + name);
        }
        return RelOptUtil.createCastRel(expanded, expected, true);
    }

    private static PlannerException invalid(String message) {
        return new PlannerException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE, message);
    }

    private static boolean containsFrozenInput(RelNode relation) {
        if (relation instanceof TableScan scan) {
            return PlanTable.require(scan).unwrap(ConnectorReadBinding.class) != null;
        }
        return relation.getInputs().stream().anyMatch(DerivedQueryExpansion::containsFrozenInput);
    }

    private record Identity(List<String> name, String sourceIdentity) {}
}
