package org.pangrova.parser;

import org.pangrova.parser.ast.CreateCatalogStatement;
import org.pangrova.parser.ast.CreateDynamicTableStatement;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.CreateMaterializedViewStatement;
import org.pangrova.parser.ast.InsertStatement;
import org.pangrova.parser.ast.ShowSchemasStatement;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.*;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.ast.statement.maintenance.CallStatement;
import org.pangrova.parser.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.ast.statement.stream.DescribeStreamStatement;
import org.pangrova.parser.ast.statement.stream.DropStreamStatement;
import org.pangrova.parser.ast.statement.stream.ShowStreamsStatement;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Prints DDL, DML, Dynamic Table, and Show/Describe statements as SQL text.
 * Delegates expression and query printing to {@link AstPrinter}.
 */
final class StatementPrinter {

    private final AstPrinter printer;
    private final StreamStatementPrinter streamPrinter;
    private final SemanticFeatureStatementPrinter semanticFeaturePrinter;
    private final IndexStatementPrinter indexPrinter;

    StatementPrinter(AstPrinter printer) {
        this.printer = printer;
        this.streamPrinter = new StreamStatementPrinter(printer);
        this.semanticFeaturePrinter = new SemanticFeatureStatementPrinter(printer);
        this.indexPrinter = new IndexStatementPrinter(printer);
    }

    Map<Class<? extends Statement>, Function<Statement, String>> buildDispatch() {
        return Map.ofEntries(
                // Catalog
                Map.entry(CreateCatalogStatement.class, s -> printCreateCatalog((CreateCatalogStatement) s)),
                Map.entry(DropCatalogStatement.class, s -> "DROP CATALOG " + printer.quote(((DropCatalogStatement) s).name())),
                Map.entry(ShowCatalogsStatement.class, s -> "SHOW CATALOGS"),
                Map.entry(DescribeCatalogStatement.class, s -> "DESCRIBE CATALOG " + printer.quote(((DescribeCatalogStatement) s).name())),
                Map.entry(AlterCatalogStatement.class, s -> printAlterCatalog((AlterCatalogStatement) s)),
                Map.entry(CallStatement.class, s -> printCall((CallStatement) s)),
                // Dynamic Table
                Map.entry(CreateDynamicTableStatement.class, s -> printCreateDynamicTable((CreateDynamicTableStatement) s)),
                Map.entry(AlterDynamicTableStatement.class, s -> printAlterDynamicTable((AlterDynamicTableStatement) s)),
                Map.entry(DropDynamicTableStatement.class, s -> printDropDynamicTable((DropDynamicTableStatement) s)),
                Map.entry(ShowDynamicTablesStatement.class, s -> "SHOW DYNAMIC TABLES"),
                Map.entry(DescribeDynamicTableStatement.class, s -> "DESCRIBE DYNAMIC TABLE " + printer.printId(((DescribeDynamicTableStatement) s).name())),
                // Materialized View
                Map.entry(CreateMaterializedViewStatement.class,
                        s -> printCreateMaterializedView((CreateMaterializedViewStatement) s)),
                Map.entry(AlterMaterializedViewStatement.class,
                        s -> printAlterMaterializedView((AlterMaterializedViewStatement) s)),
                Map.entry(RefreshMaterializedViewStatement.class, s -> "REFRESH MATERIALIZED VIEW "
                        + printer.printId(((RefreshMaterializedViewStatement) s).name())),
                Map.entry(DropMaterializedViewStatement.class, s -> printDropMaterializedView(
                        (DropMaterializedViewStatement) s)),
                Map.entry(ShowMaterializedViewsStatement.class,
                        s -> printShowMaterializedViews((ShowMaterializedViewsStatement) s)),
                Map.entry(DescribeMaterializedViewStatement.class, s -> "DESCRIBE MATERIALIZED VIEW "
                        + printer.printId(((DescribeMaterializedViewStatement) s).name())),
                // Materialization observability
                Map.entry(ShowMaterializationsStatement.class, s -> "SHOW MATERIALIZATIONS"),
                Map.entry(ShowMaterializationRefreshHistoryStatement.class,
                        s -> printScopedShow("SHOW MATERIALIZATION REFRESH HISTORY",
                                ((ShowMaterializationRefreshHistoryStatement) s).object())),
                Map.entry(ShowMaterializationDependenciesStatement.class,
                        s -> printScopedShow("SHOW MATERIALIZATION DEPENDENCIES",
                                ((ShowMaterializationDependenciesStatement) s).object())),
                Map.entry(ShowMaterializationGcHistoryStatement.class,
                        s -> printScopedShow("SHOW MATERIALIZATION GC HISTORY",
                                ((ShowMaterializationGcHistoryStatement) s).object())),
                // DDL
                Map.entry(CreateTableStatement.class, s -> printCreateTable((CreateTableStatement) s)),
                Map.entry(DropTableStatement.class, s -> printDropTable((DropTableStatement) s)),
                Map.entry(AlterTableStatement.AddColumn.class, s -> printAlterTableAddColumn((AlterTableStatement.AddColumn) s)),
                Map.entry(AlterTableStatement.DropColumn.class, s -> printAlterTableDropColumn((AlterTableStatement.DropColumn) s)),
                Map.entry(AlterTableStatement.RenameColumn.class, s -> printAlterTableRenameColumn((AlterTableStatement.RenameColumn) s)),
                Map.entry(AlterTableStatement.ChangeColumnType.class, s -> printAlterTableChangeType((AlterTableStatement.ChangeColumnType) s)),
                Map.entry(AlterTableStatement.SetPartitionSpec.class, s -> printAlterTableSetPartitionSpec((AlterTableStatement.SetPartitionSpec) s)),
                Map.entry(AlterTableStatement.SetSortOrder.class, s -> printAlterTableSetSortOrder((AlterTableStatement.SetSortOrder) s)),
                Map.entry(CreateSchemaStatement.class, s -> printCreateSchema((CreateSchemaStatement) s)),
                Map.entry(CreateSemanticViewStatement.class,
                        s -> semanticFeaturePrinter.printCreateSemanticView((CreateSemanticViewStatement) s)),
                Map.entry(PublishSemanticViewStatement.class,
                        s -> semanticFeaturePrinter.printPublishSemanticView((PublishSemanticViewStatement) s)),
                Map.entry(CreateFeatureViewStatement.class,
                        s -> semanticFeaturePrinter.printCreateFeatureView((CreateFeatureViewStatement) s)),
                Map.entry(PublishFeatureViewStatement.class,
                        s -> semanticFeaturePrinter.printPublishFeatureView((PublishFeatureViewStatement) s)),
                Map.entry(MaterializeFeatureViewStatement.class,
                        s -> semanticFeaturePrinter.printMaterializeFeatureView((MaterializeFeatureViewStatement) s)),
                // DML
                Map.entry(InsertStatement.class, s -> printInsert((InsertStatement) s)),
                Map.entry(InsertSelectStatement.class, s -> printInsertSelect((InsertSelectStatement) s)),
                Map.entry(InsertOverwriteStatement.class, s -> printInsertOverwrite((InsertOverwriteStatement) s)),
                Map.entry(DeleteStatement.class, s -> printDelete((DeleteStatement) s)),
                Map.entry(UpdateStatement.class, s -> printUpdate((UpdateStatement) s)),
                Map.entry(MergeStatement.class, s -> printMerge((MergeStatement) s)),
                // Show/Describe
                Map.entry(ShowTablesStatement.class, s -> printShowTables((ShowTablesStatement) s)),
                Map.entry(ShowSchemasStatement.class, s -> printShowSchemas((ShowSchemasStatement) s)),
                Map.entry(DescribeTableStatement.class, s -> "DESCRIBE " + printer.printId(((DescribeTableStatement) s).tableName())),
                Map.entry(ShowSemanticViewsStatement.class,
                        s -> semanticFeaturePrinter.printShowSemanticViews((ShowSemanticViewsStatement) s)),
                // Index DDL
                Map.entry(CreateIndexStatement.class, s -> indexPrinter.printCreateIndex((CreateIndexStatement) s)),
                Map.entry(DropIndexStatement.class, s -> indexPrinter.printDropIndex((DropIndexStatement) s)),
                Map.entry(ShowIndexesStatement.class, s -> "SHOW INDEXES ON " + printer.printId(((ShowIndexesStatement) s).tableName())),
                Map.entry(AlterIndexStatement.class, s -> indexPrinter.printAlterIndex((AlterIndexStatement) s)),
                // Clustering DDL
                Map.entry(SetClusteringOrderStatement.class, s -> printSetClusteringOrder((SetClusteringOrderStatement) s)),
                Map.entry(ReclusterTableStatement.class, s -> printReclusterTable((ReclusterTableStatement) s)),
                Map.entry(ShowClusteringStatusStatement.class, s -> "SHOW CLUSTERING STATUS FOR TABLE " + printer.printId(((ShowClusteringStatusStatement) s).tableName())),
                // Stream DDL
                Map.entry(CreateStreamStatement.class, s -> streamPrinter.printCreateStream((CreateStreamStatement) s)),
                Map.entry(AlterStreamStatement.class, s -> streamPrinter.printAlterStream((AlterStreamStatement) s)),
                Map.entry(DropStreamStatement.class, s -> streamPrinter.printDropStream((DropStreamStatement) s)),
                Map.entry(ShowStreamsStatement.class, s -> "SHOW STREAMS"),
                Map.entry(DescribeStreamStatement.class, s -> "DESCRIBE STREAM " + printer.printId(((DescribeStreamStatement) s).name())),
                // Incremental Read
                Map.entry(IncrementalReadStatement.class, s -> printIncrementalRead((IncrementalReadStatement) s)),
                // Show Metadata
                Map.entry(ShowMetadataStatement.ShowSnapshots.class, s -> "SHOW SNAPSHOTS FROM " + ((ShowMetadataStatement) s).tableName().stream().map(printer::quote).collect(Collectors.joining("."))),
                Map.entry(ShowMetadataStatement.ShowFiles.class, s -> "SHOW FILES FROM " + ((ShowMetadataStatement) s).tableName().stream().map(printer::quote).collect(Collectors.joining("."))),
                Map.entry(ShowMetadataStatement.ShowManifests.class, s -> "SHOW MANIFESTS FROM " + ((ShowMetadataStatement) s).tableName().stream().map(printer::quote).collect(Collectors.joining("."))),
                Map.entry(ShowMetadataStatement.ShowPartitions.class, s -> "SHOW PARTITIONS FROM " + ((ShowMetadataStatement) s).tableName().stream().map(printer::quote).collect(Collectors.joining("."))),
                // Backfill Operations
                Map.entry(BackfillColumnStatement.class, s -> printBackfillColumn((BackfillColumnStatement) s)),
                Map.entry(CancelBackfillStatement.class, s -> printCancelBackfill((CancelBackfillStatement) s)),
                Map.entry(ShowBackfillStatusStatement.class, s -> "SHOW BACKFILL STATUS ON " + printer.printId(((ShowBackfillStatusStatement) s).tableName()))
        );
    }

    private String printCreateCatalog(CreateCatalogStatement s) {
        var sb = new StringBuilder("CREATE CATALOG " + printer.quote(s.name()));
        s.connectorType().ifPresent(ct -> sb.append(" USING ").append(printer.quote(ct)));
        sb.append(" WITH PROPERTIES (").append(printer.printProps(s.properties())).append(")");
        return sb.toString();
    }

    private String printAlterCatalog(AlterCatalogStatement s) {
        return "ALTER CATALOG " + printer.quote(s.name())
                + " SET PROPERTIES (" + printer.printProps(s.properties()) + ")";
    }
    private String printCall(CallStatement s) {
        return "CALL " + printer.printId(s.procedureName())
                + "(" + printer.joinExprs(s.arguments()) + ")";
    }

    private String printCreateDynamicTable(CreateDynamicTableStatement s) {
        var sb = new StringBuilder("CREATE DYNAMIC TABLE ");
        sb.append(printer.printId(s.name()));
        appendTargetLag(sb, s.targetLag());
        s.refreshMode().ifPresent(rm -> sb.append(" REFRESH_MODE = ").append(rm.name()));
        s.incrementalStrategy().ifPresent(strategy -> sb.append(" INCREMENTAL_STRATEGY = ")
                .append(strategy.name()));
        s.warehouse().ifPresent(w -> sb.append(" WAREHOUSE = '")
                .append(w.replace("'", "''")).append("'"));
        sb.append(" AS ").append(printer.printQuery(s.query()));
        if (!s.properties().isEmpty()) {
            sb.append(" WITH (");
            sb.append(s.properties().entrySet().stream()
                    .map(e -> "'" + e.getKey().replace("'", "''") + "' = '"
                            + e.getValue().replace("'", "''") + "'")
                    .collect(Collectors.joining(", ")));
            sb.append(")");
        }
        return sb.toString();
    }

    private String printCreateMaterializedView(CreateMaterializedViewStatement s) {
        var sb = new StringBuilder("CREATE ");
        if (s.orReplace()) sb.append("OR REPLACE ");
        sb.append("MATERIALIZED VIEW ");
        if (s.ifNotExists()) sb.append("IF NOT EXISTS ");
        sb.append(printer.printId(s.name()));
        appendTargetLag(sb, s.targetLag());
        s.refreshMode().ifPresent(rm -> sb.append(" REFRESH_MODE = ").append(rm.name()));
        s.incrementalStrategy().ifPresent(strategy -> sb.append(" INCREMENTAL_STRATEGY = ")
                .append(strategy.name()));
        sb.append(" AS ").append(printer.printQuery(s.query()));
        return sb.toString();
    }

    private String printDropMaterializedView(DropMaterializedViewStatement s) {
        var sb = new StringBuilder("DROP MATERIALIZED VIEW ");
        if (s.ifExists()) sb.append("IF EXISTS ");
        sb.append(printer.printId(s.name()));
        if (s.cascade()) sb.append(" CASCADE");
        return sb.toString();
    }

    private String printAlterMaterializedView(AlterMaterializedViewStatement s) {
        return "ALTER MATERIALIZED VIEW " + printer.printId(s.name())
                + " " + printMvAction(s.action());
    }

    private String printShowMaterializedViews(ShowMaterializedViewsStatement statement) {
        if (statement.pattern() == null) {
            return "SHOW MATERIALIZED VIEWS";
        }
        return "SHOW MATERIALIZED VIEWS LIKE '" + statement.pattern().replace("'", "''") + "'";
    }

    private String printScopedShow(String command, java.util.Optional<Identifier> object) {
        return object.map(name -> command + " FOR " + printer.printId(name)).orElse(command);
    }

    private static final Map<Class<? extends AlterMaterializedViewAction>, String>
            SIMPLE_MV_ACTIONS = Map.of(
                    AlterMaterializedViewAction.Suspend.class, "SUSPEND",
                    AlterMaterializedViewAction.Resume.class, "RESUME",
                    AlterMaterializedViewAction.Refresh.class, "REFRESH");

    private String printMvAction(AlterMaterializedViewAction action) {
        String simple = SIMPLE_MV_ACTIONS.get(action.getClass());
        if (simple != null) {
            return simple;
        }
        if (action instanceof AlterMaterializedViewAction.SetTargetLag stl) {
            return "SET TARGET_LAG = '" + stl.newLag() + "'";
        }
        if (action instanceof AlterMaterializedViewAction.SetRefreshMode mode) {
            return "SET REFRESH_MODE = " + mode.mode().name();
        }
        if (action instanceof AlterMaterializedViewAction.Rename rename) {
            return "RENAME TO " + printer.printId(rename.newName());
        }
        throw new IllegalArgumentException("Unknown alter action: " + action.getClass());
    }

    private static void appendTargetLag(StringBuilder sb, java.time.Duration targetLag) {
        if (!targetLag.isZero()) {
            sb.append(" TARGET_LAG = '").append(targetLag).append("'");
        }
    }

    private String printAlterDynamicTable(AlterDynamicTableStatement s) {
        var sb = new StringBuilder("ALTER DYNAMIC TABLE ");
        sb.append(printer.printId(s.name()));
        sb.append(' ').append(printAlterDtAction(s.action()));
        return sb.toString();
    }

    private static final Map<Class<? extends AlterDynamicTableAction>, String> SIMPLE_DT_ACTIONS = Map.of(
            AlterDynamicTableAction.Suspend.class, "SUSPEND",
            AlterDynamicTableAction.Resume.class, "RESUME",
            AlterDynamicTableAction.Refresh.class, "REFRESH"
    );

    private String printAlterDtAction(AlterDynamicTableAction action) {
        String simple = SIMPLE_DT_ACTIONS.get(action.getClass());
        if (simple != null) {
            return simple;
        }
        if (action instanceof AlterDynamicTableAction.SetTargetLag stl) {
            return "SET TARGET_LAG = '" + stl.newLag() + "'";
        }
        if (action instanceof AlterDynamicTableAction.SetRefreshMode mode) {
            return "SET REFRESH_MODE = " + mode.mode().name();
        }
        if (action instanceof AlterDynamicTableAction.Rename rename) {
            return "RENAME TO " + printer.printId(rename.newName());
        }
        if (action instanceof AlterDynamicTableAction.Redefine r) {
            return "REDEFINE AS " + printer.printQuery(r.newQuery());
        }
        if (action instanceof AlterDynamicTableAction.BackfillEmbedding be) {
            return "BACKFILL EMBEDDING " + printer.quote(be.columnName());
        }
        throw new IllegalArgumentException("Unknown alter action: " + action.getClass());
    }

    private String printDropDynamicTable(DropDynamicTableStatement s) {
        var sb = new StringBuilder("DROP DYNAMIC TABLE ");
        sb.append(printer.printId(s.name()));
        if (s.cascade()) sb.append(" CASCADE");
        return sb.toString();
    }

    private String printCreateTable(CreateTableStatement s) {
        var sb = new StringBuilder("CREATE TABLE ");
        if (s.ifNotExists()) sb.append("IF NOT EXISTS ");
        sb.append(printer.printId(s.tableName()));
        sb.append(" (");
        sb.append(s.columns().stream()
                .map(col -> printer.quote(col.name()) + " " + col.type().toCanonicalString())
                .collect(Collectors.joining(", ")));
        appendPrimaryKey(sb, s.primaryKeyColumns());
        sb.append(")");
        appendTableClauses(sb, s);
        return sb.toString();
    }

    private void appendPrimaryKey(StringBuilder sb, List<String> pkColumns) {
        if (pkColumns.isEmpty()) return;
        sb.append(", PRIMARY KEY (");
        sb.append(pkColumns.stream().map(printer::quote).collect(Collectors.joining(", ")));
        sb.append(") NOT ENFORCED");
    }

    private void appendTableClauses(StringBuilder sb, CreateTableStatement s) {
        if (!s.partitionColumns().isEmpty()) {
            sb.append(" PARTITIONED BY (");
            sb.append(s.partitionColumns().stream().map(printer::quote).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        if (!s.sortColumns().isEmpty()) {
            sb.append(" SORTED BY (");
            sb.append(s.sortColumns().stream()
                    .map(sc -> printer.quote(sc.columnName()) + (sc.ascending() ? " ASC" : " DESC"))
                    .collect(Collectors.joining(", ")));
            sb.append(")");
            if (s.useZOrder()) {
                sb.append(" USING ZORDER");
            }
        }
        s.comment().ifPresent(c -> sb.append(" COMMENT '").append(c.replace("'", "''")).append("'"));
        if (!s.properties().isEmpty()) {
            sb.append(" WITH (");
            sb.append(s.properties().entrySet().stream()
                    .map(e -> printer.quote(e.getKey()) + " = '" + e.getValue().replace("'", "''") + "'")
                    .collect(Collectors.joining(", ")));
            sb.append(")");
        }
    }

    private String printDropTable(DropTableStatement s) {
        var sb = new StringBuilder("DROP TABLE ");
        if (s.ifExists()) sb.append("IF EXISTS ");
        sb.append(printer.printId(s.tableName()));
        return sb.toString();
    }
    private String printAlterTableAddColumn(AlterTableStatement.AddColumn s) {
        var sb = new StringBuilder("ALTER TABLE ");
        sb.append(printer.printId(s.tableName()));
        sb.append(" ADD COLUMN ").append(printer.quote(s.columnName())).append(" ").append(s.columnType().toCanonicalString());
        s.defaultValue().ifPresent(def -> sb.append(" DEFAULT ").append(printer.printLiteral(def)));
        return sb.toString();
    }

    private String printAlterTableDropColumn(AlterTableStatement.DropColumn s) {
        return "ALTER TABLE " + printer.printId(s.tableName()) + " DROP COLUMN " + printer.quote(s.columnName());
    }
    private String printAlterTableRenameColumn(AlterTableStatement.RenameColumn s) {
        return "ALTER TABLE " + printer.printId(s.tableName()) + " RENAME COLUMN "
                + printer.quote(s.oldName()) + " TO " + printer.quote(s.newName());
    }
    private String printAlterTableChangeType(AlterTableStatement.ChangeColumnType s) {
        return "ALTER TABLE " + printer.printId(s.tableName()) + " ALTER COLUMN "
                + printer.quote(s.columnName()) + " TYPE " + s.newType().toCanonicalString();
    }

    private String printAlterTableSetPartitionSpec(AlterTableStatement.SetPartitionSpec s) {
        var sb = new StringBuilder("ALTER TABLE ");
        sb.append(printer.printId(s.tableName()));
        sb.append(" SET PARTITION SPEC (");
        sb.append(s.transforms().stream()
                .map(this::printTransform)
                .collect(Collectors.joining(", ")));
        sb.append(")");
        return sb.toString();
    }

    private String printTransform(AlterTableStatement.PartitionTransform t) {
        if (t.isIdentity()) {
            return printer.quote(t.columnName());
        }
        return t.functionName() + "(" + String.join(", ", t.arguments()) + ")";
    }

    private String printAlterTableSetSortOrder(AlterTableStatement.SetSortOrder s) {
        var sb = new StringBuilder("ALTER TABLE ");
        sb.append(printer.printId(s.tableName()));
        sb.append(" SET SORT ORDER (");
        sb.append(s.sortColumns().stream()
                .map(sc -> printer.quote(sc.columnName()) + (sc.ascending() ? " ASC" : " DESC"))
                .collect(Collectors.joining(", ")));
        sb.append(")");
        if (s.useZOrder()) {
            sb.append(" USING ZORDER");
        }
        return sb.toString();
    }

    private String printCreateSchema(CreateSchemaStatement s) {
        var sb = new StringBuilder("CREATE SCHEMA ");
        if (s.ifNotExists()) sb.append("IF NOT EXISTS ");
        sb.append(printer.printId(s.schemaName()));
        return sb.toString();
    }

    private String printInsert(InsertStatement s) {
        var sb = new StringBuilder("INSERT INTO ");
        sb.append(printer.printId(s.tableName()));
        s.columns().ifPresent(cols -> {
            sb.append(" (");
            sb.append(cols.stream().map(printer::quote).collect(Collectors.joining(", ")));
            sb.append(")");
        });
        sb.append(" VALUES ");
        sb.append(s.values().stream()
                .map(row -> "(" + printer.joinExprs(row) + ")")
                .collect(Collectors.joining(", ")));
        return sb.toString();
    }

    private String printInsertSelect(InsertSelectStatement s) {
        var sb = new StringBuilder(s.overwrite() ? "INSERT OVERWRITE " : "INSERT INTO ");
        sb.append(printer.printId(s.tableName()));
        s.columns().ifPresent(cols -> {
            sb.append(" (");
            sb.append(cols.stream().map(printer::quote).collect(Collectors.joining(", ")));
            sb.append(")");
        });
        sb.append(" ").append(printer.printSetQuery(s.selectQuery()));
        return sb.toString();
    }

    private String printInsertOverwrite(InsertOverwriteStatement s) {
        var sb = new StringBuilder("INSERT OVERWRITE ");
        sb.append(printer.printId(s.tableName()));
        s.columns().ifPresent(cols -> {
            sb.append(" (");
            sb.append(cols.stream().map(printer::quote).collect(Collectors.joining(", ")));
            sb.append(")");
        });
        s.partitionSpec().ifPresent(spec -> {
            sb.append(" PARTITION (");
            sb.append(spec.entrySet().stream()
                    .map(e -> printer.quote(e.getKey()) + " = " + printer.printLiteral(e.getValue()))
                    .collect(Collectors.joining(", ")));
            sb.append(")");
        });
        sb.append(" VALUES ");
        sb.append(s.values().stream()
                .map(row -> "(" + printer.joinExprs(row) + ")")
                .collect(Collectors.joining(", ")));
        return sb.toString();
    }

    private String printDelete(DeleteStatement s) {
        var sb = new StringBuilder("DELETE FROM ");
        sb.append(printer.printId(s.tableName()));
        s.where().ifPresent(w -> sb.append(" WHERE ").append(printer.printExpr(w)));
        return sb.toString();
    }
    private String printUpdate(UpdateStatement s) {
        var sb = new StringBuilder("UPDATE ");
        sb.append(printer.printId(s.tableName()));
        sb.append(" SET ");
        sb.append(s.assignments().stream()
                .map(a -> printer.quote(a.column()) + " = " + printer.printExpr(a.value()))
                .collect(Collectors.joining(", ")));
        s.where().ifPresent(w -> sb.append(" WHERE ").append(printer.printExpr(w)));
        return sb.toString();
    }

    private String printMerge(MergeStatement s) {
        var sb = new StringBuilder("MERGE INTO ");
        sb.append(printer.printId(s.targetTable()));
        s.targetAlias().ifPresent(a -> sb.append(" AS ").append(printer.quote(a)));
        sb.append(" USING ").append(printer.printRef(s.sourceTable()));
        sb.append(" ON ").append(printer.printExpr(s.onCondition()));
        for (var clause : s.clauses()) {
            sb.append(" ").append(printMergeClause(clause));
        }
        return sb.toString();
    }

    private String printMergeClause(MergeStatement.MergeClause clause) {
        if (clause instanceof MergeStatement.MergeClause.WhenMatchedUpdate u) {
            return printWhenMatchedUpdate(u);
        }
        if (clause instanceof MergeStatement.MergeClause.WhenMatchedDelete d) {
            return printWhenMatchedDelete(d);
        }
        if (clause instanceof MergeStatement.MergeClause.WhenNotMatchedInsert i) {
            return printWhenNotMatchedInsert(i);
        }
        throw new IllegalArgumentException("Unknown merge clause: " + clause.getClass());
    }

    private String printWhenMatchedUpdate(MergeStatement.MergeClause.WhenMatchedUpdate u) {
        var sb = new StringBuilder("WHEN MATCHED");
        u.condition().ifPresent(c -> sb.append(" AND ").append(printer.printExpr(c)));
        sb.append(" THEN UPDATE SET ");
        sb.append(u.assignments().stream()
                .map(a -> printer.quote(a.column()) + " = " + printer.printExpr(a.value()))
                .collect(Collectors.joining(", ")));
        return sb.toString();
    }

    private String printWhenMatchedDelete(MergeStatement.MergeClause.WhenMatchedDelete d) {
        var sb = new StringBuilder("WHEN MATCHED");
        d.condition().ifPresent(c -> sb.append(" AND ").append(printer.printExpr(c)));
        sb.append(" THEN DELETE");
        return sb.toString();
    }

    private String printWhenNotMatchedInsert(MergeStatement.MergeClause.WhenNotMatchedInsert i) {
        var sb = new StringBuilder("WHEN NOT MATCHED");
        i.condition().ifPresent(c -> sb.append(" AND ").append(printer.printExpr(c)));
        sb.append(" THEN INSERT");
        i.columns().ifPresent(cols -> {
            sb.append(" (");
            sb.append(cols.stream().map(printer::quote).collect(Collectors.joining(", ")));
            sb.append(")");
        });
        sb.append(" VALUES (").append(printer.joinExprs(i.values())).append(")");
        return sb.toString();
    }

    private String printShowTables(ShowTablesStatement s) {
        if (s.qualifiedName().isEmpty()) {
            return "SHOW TABLES";
        }
        return "SHOW TABLES FROM " + s.qualifiedName().stream().map(printer::quote).collect(Collectors.joining("."));
    }
    private String printShowSchemas(ShowSchemasStatement s) {
        return "SHOW SCHEMAS FROM " + printer.quote(s.catalogName());
    }

    private String printSetClusteringOrder(SetClusteringOrderStatement s) {
        var sb = new StringBuilder("ALTER TABLE ");
        sb.append(printer.printId(s.tableName()));
        sb.append(" SET CLUSTERING ORDER (");
        sb.append(s.columnNames().stream().map(printer::quote).collect(Collectors.joining(", ")));
        sb.append(")");
        if (s.mode() != null) {
            sb.append(" MODE ").append(s.mode());
        }
        return sb.toString();
    }

    private String printReclusterTable(ReclusterTableStatement s) {
        var sb = new StringBuilder("RECLUSTER TABLE ");
        sb.append(printer.printId(s.tableName()));
        s.whereCondition().ifPresent(w -> sb.append(" WHERE ").append(printer.printExpr(w)));
        return sb.toString();
    }
    private String printBackfillColumn(BackfillColumnStatement s) {
        var sb = new StringBuilder("ALTER TABLE ");
        sb.append(printer.printId(s.tableName()));
        sb.append(" BACKFILL COLUMN ").append(printer.quote(s.columnName()));
        s.whereClause().ifPresent(w -> sb.append(" WHERE ").append(printer.printExpr(w)));
        s.batchSize().ifPresent(b -> sb.append(" BATCH_SIZE ").append(b));
        s.concurrency().ifPresent(c -> sb.append(" CONCURRENCY ").append(c));
        return sb.toString();
    }

    private String printCancelBackfill(CancelBackfillStatement s) {
        return "CANCEL BACKFILL ON " + printer.printId(s.tableName())
                + " COLUMN " + printer.quote(s.columnName());
    }
    private String printIncrementalRead(IncrementalReadStatement s) {
        var sb = new StringBuilder("SELECT ");
        sb.append(printer.joinItems(s.selectItems()));
        sb.append(" FROM ");
        sb.append(s.tableName().stream().map(printer::quote).collect(Collectors.joining(".")));
        sb.append(" CHANGES BETWEEN SNAPSHOT ");
        sb.append(printer.printExpr(s.fromSnapshot()));
        sb.append(" AND ");
        sb.append(printer.printExpr(s.toSnapshot()));
        s.where().ifPresent(w -> sb.append(" WHERE ").append(printer.printExpr(w)));
        if (!s.orderBy().isEmpty()) {
            sb.append(" ORDER BY ").append(printer.joinSortItems(s.orderBy()));
        }
        s.limit().ifPresent(l -> sb.append(" LIMIT ").append(printer.printExpr(l)));
        return sb.toString();
    }
}
