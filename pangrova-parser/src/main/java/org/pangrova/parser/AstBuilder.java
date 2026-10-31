package org.pangrova.parser;

import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.TimeTravelSpec.Type;
import org.pangrova.parser.ast.RerankClause;
import org.pangrova.parser.ast.RerankClause.CascadeStrategy;
import org.pangrova.parser.ast.RerankClause.CrossEncoderStrategy;
import org.pangrova.parser.ast.RerankClause.LinearCombinationStrategy;
import org.pangrova.parser.ast.RerankClause.RerankStrategy;
import org.pangrova.parser.ast.RerankClause.RrfStrategy;
import org.pangrova.parser.ast.statement.AlterCatalogStatement;
import org.pangrova.parser.ast.statement.AlterDynamicTableStatement;
import org.pangrova.parser.ast.statement.AlterMaterializedViewStatement;
import org.pangrova.parser.ast.statement.CreateSchemaStatement;
import org.pangrova.parser.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.DescribeCatalogStatement;
import org.pangrova.parser.ast.statement.DescribeDynamicTableStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationDependenciesStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationGcHistoryStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationRefreshHistoryStatement;
import org.pangrova.parser.ast.statement.ShowMaterializationsStatement;
import org.pangrova.parser.ast.statement.DescribeMaterializedViewStatement;
import org.pangrova.parser.ast.statement.DescribeTableStatement;
import org.pangrova.parser.ast.statement.DropDynamicTableStatement;
import org.pangrova.parser.ast.statement.DropCatalogStatement;
import org.pangrova.parser.ast.statement.DropMaterializedViewStatement;
import org.pangrova.parser.ast.statement.DropTableStatement;
import org.pangrova.parser.ast.statement.RefreshMaterializedViewStatement;
import org.pangrova.parser.ast.statement.ShowMaterializedViewsStatement;
import org.pangrova.parser.ast.statement.dml.MergeStatement;
import org.pangrova.parser.ast.statement.ShowDynamicTablesStatement;
import org.pangrova.parser.ast.statement.ShowCatalogsStatement;
import org.pangrova.parser.ast.statement.ShowTableControlsStatement;
import org.pangrova.parser.ast.statement.ShowTablesStatement;
import org.pangrova.parser.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.ast.statement.dml.UpdateStatement;
import org.pangrova.parser.ast.statement.maintenance.CallStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.pangrova.parser.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.ast.statement.stream.DescribeStreamStatement;
import org.pangrova.parser.ast.statement.stream.DropStreamStatement;
import org.pangrova.parser.ast.statement.stream.ShowStreamsStatement;
import org.pangrova.parser.ast.statement.AlterIndexStatement;
import org.pangrova.parser.ast.statement.CreateIndexStatement;
import org.pangrova.parser.ast.statement.DropIndexStatement;
import org.pangrova.parser.ast.statement.ShowIndexesStatement;
import org.pangrova.parser.ast.statement.SetClusteringOrderStatement;
import org.pangrova.parser.ast.statement.ReclusterTableStatement;
import org.pangrova.parser.ast.statement.ShowClusteringStatusStatement;
import org.pangrova.parser.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.ast.statement.CreateViewStatement;
import org.pangrova.parser.ast.statement.DropViewStatement;
import org.pangrova.parser.ast.statement.ShowViewsStatement;
import org.pangrova.parser.ast.statement.ShowMetadataStatement;
import org.pangrova.parser.ast.statement.IncrementalReadStatement;

import java.util.List;
import java.util.Optional;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;

/**
 * ANTLR4 visitor that converts parse tree into AST nodes.
 * DDL/DML visitor methods are delegated to {@link AstDdlDmlBuilder}.
 * Expression and literal visitor methods are delegated to {@link AstExpressionBuilder}.
 *
 * <p>During visitation, source location information is extracted from each
 * {@link ParserRuleContext} and registered in the {@link SourceLocationRegistry},
 * enabling downstream validators and planners to report precise error locations.</p>
 */
public class AstBuilder extends PangrovaSqlBaseVisitor<Object> {

    private final CommonTokenStream tokenStream;
    private final SqlDialect dialect;
    private final AstDdlDmlBuilder ddlDml = new AstDdlDmlBuilder(this);
    private final AstExtendedDdlBuilder extDdl = new AstExtendedDdlBuilder(this);
    private final AstSemanticViewBuilder semanticView = new AstSemanticViewBuilder(this);
    private final AstFeatureViewBuilder featureView = new AstFeatureViewBuilder(this);
    private final AstDmlBuilder dml = new AstDmlBuilder(this, ddlDml);
    private final AstExpressionBuilder expr = new AstExpressionBuilder(this);
    private final SourceLocationRegistry locationRegistry = new SourceLocationRegistry();

    AstBuilder(CommonTokenStream tokenStream) {
        this(tokenStream, SqlDialect.PANGROVA);
    }

    AstBuilder(CommonTokenStream tokenStream, SqlDialect dialect) {
        this.tokenStream = tokenStream;
        this.dialect = dialect;
    }

    CommonTokenStream tokenStream() {
        return tokenStream;
    }

    /** Parses a property list whose values are consumed as text, not literals. */
    java.util.Map<String, String> propertyTextMap(PangrovaSqlParser.PropertyListContext ctx) {
        return ddlDml.propertyTextMap(ctx);
    }

    /**
     * Returns the registry containing source locations for all AST nodes built
     * during this parse session.
     */
    public SourceLocationRegistry locationRegistry() {
        return locationRegistry;
    }

    /**
     * Extracts source location from a ParserRuleContext and creates a SourceLocation.
     * Returns null if the context or its tokens are null (e.g., error recovery).
     */
    SourceLocation extractLocation(ParserRuleContext ctx) {
        if (ctx == null) {
            return null;
        }
        Token start = ctx.getStart();
        Token stop = ctx.getStop();
        if (start == null) {
            return null;
        }
        int line = start.getLine();
        int col = start.getCharPositionInLine() + 1; // convert 0-based to 1-based
        int startTok = start.getTokenIndex();
        int endTok = stop != null ? stop.getTokenIndex() : startTok;
        return new SourceLocation(line, col, startTok, endTok);
    }

    /**
     * Registers the source location for an AST node built from the given context.
     * Returns the node unchanged for fluent usage.
     */
    <T> T withLocation(T node, ParserRuleContext ctx) {
        SourceLocation loc = extractLocation(ctx);
        locationRegistry.register(node, loc);
        return node;
    }

    @Override
    public Statement visitSingleStatement(PangrovaSqlParser.SingleStatementContext ctx) {
        Statement stmt = (Statement) visit(ctx.statement());
        return withLocation(stmt, ctx);
    }
    @Override public Statement visitCteQueryStatement(PangrovaSqlParser.CteQueryStatementContext ctx) { return withLocation((Statement) visit(ctx.cteQuery()), ctx); }
    @Override public Statement visitQueryStatement(PangrovaSqlParser.QueryStatementContext ctx) { return withLocation((Statement) visit(ctx.setQuery()), ctx); }
    @Override public Statement visitExplainStmt(PangrovaSqlParser.ExplainStmtContext ctx) {
        Statement inner = (Statement) visit(ctx.explainStatement().statement());
        return withLocation(new org.pangrova.parser.ast.statement.ExplainStatement(inner), ctx);
    }

    @Override
    public Statement visitCteQuery(PangrovaSqlParser.CteQueryContext ctx) {
        boolean recursive = ctx.RECURSIVE() != null;
        List<CteQuery.CteDefinition> ctes = ctx.cteDefinition().stream()
                .map(this::buildCteDefinition)
                .toList();
        return withLocation(new CteQuery(recursive, ctes, (Statement) visit(ctx.setQuery())), ctx);
    }

    private CteQuery.CteDefinition buildCteDefinition(PangrovaSqlParser.CteDefinitionContext cd) {
        String name = extractIdentifierText(cd.identifier());
        List<String> columnList = cd.columnNameList() != null
                ? cd.columnNameList().identifier().stream().map(this::extractIdentifierText).toList()
                : List.of();
        Statement query = (Statement) visit(cd.setQuery());
        java.util.Optional<CteQuery.SearchClause> search = cd.cteSearchClause() != null
                ? java.util.Optional.of(buildSearchClause(cd.cteSearchClause()))
                : java.util.Optional.empty();
        java.util.Optional<CteQuery.CycleClause> cycle = cd.cteCycleClause() != null
                ? java.util.Optional.of(buildCycleClause(cd.cteCycleClause()))
                : java.util.Optional.empty();
        return new CteQuery.CteDefinition(name, columnList, query, search, cycle);
    }

    private CteQuery.SearchClause buildSearchClause(PangrovaSqlParser.CteSearchClauseContext ctx) {
        CteQuery.SearchOrder order = ctx.DEPTH() != null
                ? CteQuery.SearchOrder.DEPTH_FIRST
                : CteQuery.SearchOrder.BREADTH_FIRST;
        List<SortItem> orderBy = ctx.orderByList().sortItem().stream()
                .map(si -> (SortItem) visit(si)).toList();
        String sequenceColumn = extractIdentifierText(ctx.identifier());
        return new CteQuery.SearchClause(order, orderBy, sequenceColumn);
    }

    private CteQuery.CycleClause buildCycleClause(PangrovaSqlParser.CteCycleClauseContext ctx) {
        List<String> cycleColumns = ctx.columnNameList().identifier().stream()
                .map(this::extractIdentifierText).toList();
        String cycleMarkColumn = extractIdentifierText(ctx.identifier().get(0));
        LiteralValue cycleValue = (LiteralValue) visit(ctx.literal().get(0));
        LiteralValue noCycleValue = (LiteralValue) visit(ctx.literal().get(1));
        String pathColumn = extractIdentifierText(ctx.identifier().get(1));
        return new CteQuery.CycleClause(cycleColumns, cycleMarkColumn, cycleValue, noCycleValue, pathColumn);
    }

    @Override public Statement visitCompactTableStatement(PangrovaSqlParser.CompactTableStatementContext ctx) { return (CompactStatement) ddlDml.visitCompactTable(ctx.compactTable()); }
    @Override public Statement visitOptimizeTableStatement(PangrovaSqlParser.OptimizeTableStatementContext ctx) { return (OptimizeStatement) ddlDml.visitOptimizeTable(ctx.optimizeTable()); }
    @Override public Statement visitVacuumTableStatement(PangrovaSqlParser.VacuumTableStatementContext ctx) { return (VacuumStatement) ddlDml.visitVacuumTable(ctx.vacuumTable()); }
    @Override public Statement visitAnalyzeTableStatement(PangrovaSqlParser.AnalyzeTableStatementContext ctx) { return (org.pangrova.parser.ast.statement.maintenance.AnalyzeStatement) ddlDml.visitAnalyzeTable(ctx.analyzeTable()); }
    @Override public Statement visitCreateCatalogStatement(PangrovaSqlParser.CreateCatalogStatementContext ctx) { return (CreateCatalogStatement) ddlDml.visitCreateCatalog(ctx.createCatalog()); }
    @Override public Statement visitDropCatalogStatement(PangrovaSqlParser.DropCatalogStatementContext ctx) { return (DropCatalogStatement) ddlDml.visitDropCatalog(ctx.dropCatalog()); }
    @Override public Statement visitShowCatalogsStatement(PangrovaSqlParser.ShowCatalogsStatementContext ctx) { return (ShowCatalogsStatement) ddlDml.visitShowCatalogs(ctx.showCatalogs()); }
    @Override public Statement visitDescribeCatalogStatement(PangrovaSqlParser.DescribeCatalogStatementContext ctx) { return (DescribeCatalogStatement) ddlDml.visitDescribeCatalog(ctx.describeCatalog()); }
    @Override public Statement visitAlterCatalogStatement(PangrovaSqlParser.AlterCatalogStatementContext ctx) { return (AlterCatalogStatement) ddlDml.visitAlterCatalog(ctx.alterCatalog()); }
    @Override public Statement visitCallProcedureStatement(PangrovaSqlParser.CallProcedureStatementContext ctx) { return (CallStatement) ddlDml.visitCallStatement(ctx.callStatement()); }
    @Override public Statement visitCreateDynamicTableStatement(PangrovaSqlParser.CreateDynamicTableStatementContext ctx) { return (CreateDynamicTableStatement) extDdl.visitCreateDynamicTable(ctx.createDynamicTable()); }
    @Override public Statement visitAlterDynamicTableStatement(PangrovaSqlParser.AlterDynamicTableStatementContext ctx) { return (AlterDynamicTableStatement) extDdl.visit(ctx.alterDynamicTable()); }
    @Override public Statement visitDropDynamicTableStatement(PangrovaSqlParser.DropDynamicTableStatementContext ctx) { return (DropDynamicTableStatement) extDdl.visitDropDynamicTable(ctx.dropDynamicTable()); }
    @Override public Statement visitShowDynamicTablesStatement(PangrovaSqlParser.ShowDynamicTablesStatementContext ctx) { return (ShowDynamicTablesStatement) extDdl.visitShowDynamicTables(ctx.showDynamicTables()); }
    @Override public Statement visitDescribeDynamicTableStatement(PangrovaSqlParser.DescribeDynamicTableStatementContext ctx) { return (DescribeDynamicTableStatement) extDdl.visitDescribeDynamicTable(ctx.describeDynamicTable()); }
    @Override public Statement visitCreateMaterializedViewStatement(PangrovaSqlParser.CreateMaterializedViewStatementContext ctx) { return (CreateMaterializedViewStatement) extDdl.visitCreateMaterializedView(ctx.createMaterializedView()); }
    @Override public Statement visitAlterMaterializedViewStatement(PangrovaSqlParser.AlterMaterializedViewStatementContext ctx) { return (AlterMaterializedViewStatement) extDdl.visit(ctx.alterMaterializedView()); }
    @Override public Statement visitRefreshMaterializedViewStatement(PangrovaSqlParser.RefreshMaterializedViewStatementContext ctx) { return (RefreshMaterializedViewStatement) extDdl.visitRefreshMaterializedView(ctx.refreshMaterializedView()); }
    @Override public Statement visitDropMaterializedViewStatement(PangrovaSqlParser.DropMaterializedViewStatementContext ctx) { return (DropMaterializedViewStatement) extDdl.visitDropMaterializedView(ctx.dropMaterializedView()); }
    @Override public Statement visitShowMaterializedViewsStatement(PangrovaSqlParser.ShowMaterializedViewsStatementContext ctx) { return (ShowMaterializedViewsStatement) extDdl.visitShowMaterializedViews(ctx.showMaterializedViews()); }
    @Override public Statement visitShowMaterializationsStatement(PangrovaSqlParser.ShowMaterializationsStatementContext ctx) { return (ShowMaterializationsStatement) extDdl.visitShowMaterializations(ctx.showMaterializations()); }
    @Override public Statement visitShowMaterializationRefreshHistoryStatement(PangrovaSqlParser.ShowMaterializationRefreshHistoryStatementContext ctx) { return (ShowMaterializationRefreshHistoryStatement) extDdl.visitShowMaterializationRefreshHistory(ctx.showMaterializationRefreshHistory()); }
    @Override public Statement visitShowMaterializationDependenciesStatement(PangrovaSqlParser.ShowMaterializationDependenciesStatementContext ctx) { return (ShowMaterializationDependenciesStatement) extDdl.visitShowMaterializationDependencies(ctx.showMaterializationDependencies()); }
    @Override public Statement visitShowMaterializationGcHistoryStatement(PangrovaSqlParser.ShowMaterializationGcHistoryStatementContext ctx) { return (ShowMaterializationGcHistoryStatement) extDdl.visitShowMaterializationGcHistory(ctx.showMaterializationGcHistory()); }
    @Override public Statement visitDescribeMaterializedViewStatement(PangrovaSqlParser.DescribeMaterializedViewStatementContext ctx) { return (DescribeMaterializedViewStatement) extDdl.visitDescribeMaterializedView(ctx.describeMaterializedView()); }
    @Override public Statement visitCreateTableStatement(PangrovaSqlParser.CreateTableStatementContext ctx) { return withLocation((Statement) ddlDml.visit(ctx.createTable()), ctx); }
    @Override public Statement visitDropTableStatement(PangrovaSqlParser.DropTableStatementContext ctx) { return withLocation((DropTableStatement) ddlDml.visitDropTable(ctx.dropTable()), ctx); }
    @Override public Statement visitInsertIntoStatement(PangrovaSqlParser.InsertIntoStatementContext ctx) { return withLocation((InsertStatement) dml.visitInsertInto(ctx.insertInto()), ctx); }
    @Override public Statement visitInsertOverwriteStatement(PangrovaSqlParser.InsertOverwriteStatementContext ctx) { return withLocation((InsertOverwriteStatement) dml.visitInsertOverwrite(ctx.insertOverwrite()), ctx); }
    @Override public Statement visitInsertSelectStatement(PangrovaSqlParser.InsertSelectStatementContext ctx) { return withLocation((InsertSelectStatement) dml.visitInsertSelect(ctx.insertSelect()), ctx); }
    @Override public Statement visitCopyIntoStatement(PangrovaSqlParser.CopyIntoStatementContext ctx) { return withLocation((InsertSelectStatement) dml.visitCopyInto(ctx.copyInto()), ctx); }
    @Override public Statement visitShowSchemasStatement(PangrovaSqlParser.ShowSchemasStatementContext ctx) { return (ShowSchemasStatement) ddlDml.visitShowSchemas(ctx.showSchemas()); }
    @Override public Statement visitShowTablesStatement(PangrovaSqlParser.ShowTablesStatementContext ctx) { return (ShowTablesStatement) ddlDml.visitShowTables(ctx.showTables()); }
    @Override public Statement visitShowTableControlsStatement(PangrovaSqlParser.ShowTableControlsStatementContext ctx) { return (ShowTableControlsStatement) ddlDml.visitShowTableControls(ctx.showTableControls()); }
    @Override public Statement visitShowViewsStatement(PangrovaSqlParser.ShowViewsStatementContext ctx) { return (ShowViewsStatement) ddlDml.visitShowViews(ctx.showViews()); }
    @Override public Statement visitShowSemanticViewsStatement(PangrovaSqlParser.ShowSemanticViewsStatementContext ctx) { return semanticView.visitShowSemanticViews(ctx.showSemanticViews()); }
    @Override public Statement visitShowSnapshotsStatement(PangrovaSqlParser.ShowSnapshotsStatementContext ctx) { return (ShowMetadataStatement) ddlDml.visitShowSnapshots(ctx.showSnapshots()); }
    @Override public Statement visitShowFilesStatement(PangrovaSqlParser.ShowFilesStatementContext ctx) { return (ShowMetadataStatement) ddlDml.visitShowFiles(ctx.showFiles()); }
    @Override public Statement visitShowManifestsStatement(PangrovaSqlParser.ShowManifestsStatementContext ctx) { return (ShowMetadataStatement) ddlDml.visitShowManifests(ctx.showManifests()); }
    @Override public Statement visitShowPartitionsStatement(PangrovaSqlParser.ShowPartitionsStatementContext ctx) { return (ShowMetadataStatement) ddlDml.visitShowPartitions(ctx.showPartitions()); }
    @Override public Statement visitDescribeTableStatement(PangrovaSqlParser.DescribeTableStatementContext ctx) { return (DescribeTableStatement) ddlDml.visitDescribeTable(ctx.describeTable()); }
    @Override public Statement visitAlterTableStatement(PangrovaSqlParser.AlterTableStatementContext ctx) {
        var a = ctx.alterTable();
        if (a instanceof PangrovaSqlParser.AlterTableCreateBranchContext
                || a instanceof PangrovaSqlParser.AlterTableDropBranchContext
                || a instanceof PangrovaSqlParser.AlterTableCreateTagContext
                || a instanceof PangrovaSqlParser.AlterTableDropTagContext
                || a instanceof PangrovaSqlParser.AlterTableAdoptContext
                || a instanceof PangrovaSqlParser.AlterTableUnlinkContext
                || a instanceof PangrovaSqlParser.AlterTableSetColumnTagContext
                || a instanceof PangrovaSqlParser.AlterTableUnsetColumnTagContext) {
            return (Statement) extDdl.visit(a);
        }
        return (Statement) ddlDml.visit(a);
    }
    @Override public Statement visitCreateViewStatement(PangrovaSqlParser.CreateViewStatementContext ctx) { return withLocation((CreateViewStatement) ddlDml.visitCreateView(ctx.createView()), ctx); }
    @Override public Statement visitCreateSemanticViewStatement(PangrovaSqlParser.CreateSemanticViewStatementContext ctx) { return withLocation(semanticView.visitCreateSemanticView(ctx.createSemanticView()), ctx); }
    @Override public Statement visitPublishSemanticViewStatement(PangrovaSqlParser.PublishSemanticViewStatementContext ctx) { return withLocation(semanticView.visitPublishSemanticView(ctx.publishSemanticView()), ctx); }
    @Override public Statement visitCreateFeatureViewStatement(PangrovaSqlParser.CreateFeatureViewStatementContext ctx) { return withLocation(featureView.visitCreateFeatureView(ctx.createFeatureView()), ctx); }
    @Override public Statement visitPublishFeatureViewStatement(PangrovaSqlParser.PublishFeatureViewStatementContext ctx) { return withLocation(featureView.visitPublishFeatureView(ctx.publishFeatureView()), ctx); }
    @Override public Statement visitMaterializeFeatureViewStatement(PangrovaSqlParser.MaterializeFeatureViewStatementContext ctx) { return withLocation(featureView.visitMaterializeFeatureView(ctx.materializeFeatureView()), ctx); }
    @Override public Statement visitDropViewStatement(PangrovaSqlParser.DropViewStatementContext ctx) { return (DropViewStatement) ddlDml.visitDropView(ctx.dropView()); }
    @Override public Statement visitCreateSchemaStatement(PangrovaSqlParser.CreateSchemaStatementContext ctx) { return (CreateSchemaStatement) ddlDml.visitCreateSchema(ctx.createSchema()); }
    @Override public Statement visitDeleteFromStatement(PangrovaSqlParser.DeleteFromStatementContext ctx) { return withLocation((DeleteStatement) dml.visitDeleteFrom(ctx.deleteFrom()), ctx); }
    @Override public Statement visitUpdateStmt(PangrovaSqlParser.UpdateStmtContext ctx) { return withLocation((UpdateStatement) dml.visitUpdateStatement(ctx.updateStatement()), ctx); }
    @Override public Statement visitMergeIntoStatement(PangrovaSqlParser.MergeIntoStatementContext ctx) { return withLocation((MergeStatement) dml.visitMergeInto(ctx.mergeInto()), ctx); }
    @Override public Statement visitCreateIndexStatement(PangrovaSqlParser.CreateIndexStatementContext ctx) { return (CreateIndexStatement) extDdl.visitCreateIndex(ctx.createIndex()); }
    @Override public Statement visitDropIndexStatement(PangrovaSqlParser.DropIndexStatementContext ctx) { return (DropIndexStatement) extDdl.visitDropIndex(ctx.dropIndex()); }
    @Override public Statement visitShowIndexesStatement(PangrovaSqlParser.ShowIndexesStatementContext ctx) { return (ShowIndexesStatement) extDdl.visitShowIndexes(ctx.showIndexes()); }
    @Override public Statement visitAlterIndexStatement(PangrovaSqlParser.AlterIndexStatementContext ctx) { return (AlterIndexStatement) extDdl.visitAlterIndex(ctx.alterIndex()); }
    @Override public Statement visitSetClusteringOrderStatement(PangrovaSqlParser.SetClusteringOrderStatementContext ctx) { return (SetClusteringOrderStatement) extDdl.visitSetClusteringOrder(ctx.setClusteringOrder()); }
    @Override public Statement visitReclusterTableStatement(PangrovaSqlParser.ReclusterTableStatementContext ctx) { return (ReclusterTableStatement) extDdl.visitReclusterTable(ctx.reclusterTable()); }
    @Override public Statement visitShowClusteringStatusStatement(PangrovaSqlParser.ShowClusteringStatusStatementContext ctx) { return (ShowClusteringStatusStatement) extDdl.visitShowClusteringStatus(ctx.showClusteringStatus()); }
    @Override public Statement visitCreateStreamStatement(PangrovaSqlParser.CreateStreamStatementContext ctx) { return (CreateStreamStatement) extDdl.visitCreateStream(ctx.createStream()); }
    @Override public Statement visitAlterStreamStatement(PangrovaSqlParser.AlterStreamStatementContext ctx) { return (AlterStreamStatement) extDdl.visit(ctx.alterStream()); }
    @Override public Statement visitDropStreamStatement(PangrovaSqlParser.DropStreamStatementContext ctx) { return (DropStreamStatement) extDdl.visitDropStream(ctx.dropStream()); }
    @Override public Statement visitShowStreamsStatement(PangrovaSqlParser.ShowStreamsStatementContext ctx) { return (ShowStreamsStatement) extDdl.visitShowStreams(ctx.showStreams()); }
    @Override public Statement visitDescribeStreamStatement(PangrovaSqlParser.DescribeStreamStatementContext ctx) { return (DescribeStreamStatement) extDdl.visitDescribeStream(ctx.describeStream()); }
    @Override public Statement visitCreateBranchStatement(PangrovaSqlParser.CreateBranchStatementContext ctx) { return (Statement) extDdl.visitCreateBranch(ctx.createBranch()); }
    @Override public Statement visitDropBranchStatement(PangrovaSqlParser.DropBranchStatementContext ctx) { return (Statement) extDdl.visitDropBranch(ctx.dropBranch()); }
    @Override public Statement visitCreateTagStatement(PangrovaSqlParser.CreateTagStatementContext ctx) { return (Statement) extDdl.visitCreateTag(ctx.createTag()); }
    @Override public Statement visitDropTagStatement(PangrovaSqlParser.DropTagStatementContext ctx) { return (Statement) extDdl.visitDropTag(ctx.dropTag()); }
    @Override public Statement visitUseBranchStatement(PangrovaSqlParser.UseBranchStatementContext ctx) { return (Statement) extDdl.visitUseBranch(ctx.useBranch()); }
    @Override public Statement visitShowBranchesStatement(PangrovaSqlParser.ShowBranchesStatementContext ctx) { return (Statement) extDdl.visitShowBranches(ctx.showBranches()); }
    @Override public Statement visitShowTagsStatement(PangrovaSqlParser.ShowTagsStatementContext ctx) { return (Statement) extDdl.visitShowTags(ctx.showTags()); }
    @Override public Statement visitCreateEmbeddingModelStatement(PangrovaSqlParser.CreateEmbeddingModelStatementContext ctx) { return (Statement) extDdl.visitCreateEmbeddingModel(ctx.createEmbeddingModel()); }
    @Override public Statement visitAlterEmbeddingModelStatement(PangrovaSqlParser.AlterEmbeddingModelStatementContext ctx) { return (Statement) extDdl.visitAlterEmbeddingModel(ctx.alterEmbeddingModel()); }
    @Override public Statement visitDropEmbeddingModelStatement(PangrovaSqlParser.DropEmbeddingModelStatementContext ctx) { return (Statement) extDdl.visitDropEmbeddingModel(ctx.dropEmbeddingModel()); }
    @Override public Statement visitShowEmbeddingModelsStatement(PangrovaSqlParser.ShowEmbeddingModelsStatementContext ctx) { return (Statement) extDdl.visitShowEmbeddingModels(ctx.showEmbeddingModels()); }
    @Override public Statement visitBackfillColumnStatement(PangrovaSqlParser.BackfillColumnStatementContext ctx) { return (Statement) extDdl.visitBackfillColumn(ctx.backfillColumn()); }
    @Override public Statement visitCancelBackfillStatement(PangrovaSqlParser.CancelBackfillStatementContext ctx) { return (Statement) extDdl.visitCancelBackfill(ctx.cancelBackfill()); }
    @Override public Statement visitShowBackfillStatusStatement(PangrovaSqlParser.ShowBackfillStatusStatementContext ctx) { return (Statement) extDdl.visitShowBackfillStatus(ctx.showBackfillStatus()); }
    @Override public Statement visitCreateStorageProfileStatement(PangrovaSqlParser.CreateStorageProfileStatementContext ctx) { return (Statement) extDdl.visitCreateStorageProfile(ctx.createStorageProfile()); }
    @Override public Statement visitDropStorageProfileStatement(PangrovaSqlParser.DropStorageProfileStatementContext ctx) { return (Statement) extDdl.visitDropStorageProfile(ctx.dropStorageProfile()); }
    @Override public Statement visitShowStorageProfilesStatement(PangrovaSqlParser.ShowStorageProfilesStatementContext ctx) { return (Statement) extDdl.visitShowStorageProfiles(ctx.showStorageProfiles()); }
    @Override public Statement visitCreateVolumeStatement(PangrovaSqlParser.CreateVolumeStatementContext ctx) { return (Statement) extDdl.visitCreateVolume(ctx.createVolume()); }
    @Override public Statement visitAlterVolumeStatement(PangrovaSqlParser.AlterVolumeStatementContext ctx) { return (Statement) extDdl.visit(ctx.alterVolume()); }
    @Override public Statement visitDropVolumeStatement(PangrovaSqlParser.DropVolumeStatementContext ctx) { return (Statement) extDdl.visitDropVolume(ctx.dropVolume()); }
    @Override public Statement visitShowVolumesStatement(PangrovaSqlParser.ShowVolumesStatementContext ctx) { return (Statement) extDdl.visitShowVolumes(ctx.showVolumes()); }
    @Override public Statement visitDescribeVolumeStatement(PangrovaSqlParser.DescribeVolumeStatementContext ctx) { return (Statement) extDdl.visitDescribeVolume(ctx.describeVolume()); }
    @Override public Statement visitCreateStageStatement(PangrovaSqlParser.CreateStageStatementContext ctx) { return (Statement) extDdl.visitCreateStage(ctx.createStage()); }
    @Override public Statement visitAlterStageStatement(PangrovaSqlParser.AlterStageStatementContext ctx) { return (Statement) extDdl.visit(ctx.alterStage()); }
    @Override public Statement visitDropStageStatement(PangrovaSqlParser.DropStageStatementContext ctx) { return (Statement) extDdl.visitDropStage(ctx.dropStage()); }
    @Override public Statement visitShowStagesStatement(PangrovaSqlParser.ShowStagesStatementContext ctx) { return (Statement) extDdl.visitShowStages(ctx.showStages()); }
    @Override public Statement visitDescribeStageStatement(PangrovaSqlParser.DescribeStageStatementContext ctx) { return (Statement) extDdl.visitDescribeStage(ctx.describeStage()); }
    @Override public Statement visitListStageStatement(PangrovaSqlParser.ListStageStatementContext ctx) { return (Statement) extDdl.visitListStage(ctx.listStage()); }
    @Override public Statement visitRemoveStageStatement(PangrovaSqlParser.RemoveStageStatementContext ctx) { return (Statement) extDdl.visitRemoveStage(ctx.removeStage()); }

    @Override public Statement visitSetSessionStatement(PangrovaSqlParser.SetSessionStatementContext ctx) {
        var ss = ctx.setSession();
        String property;
        if (ss.propertyNameStr != null) {
            // SET 'dotted.key' = value — string literal as property name
            String raw = ss.propertyNameStr.getText();
            property = (raw.startsWith("'") && raw.endsWith("'"))
                    ? raw.substring(1, raw.length() - 1) : raw;
        } else {
            property = ss.propertyName.identifier().stream()
                    .map(this::extractIdentifierText).collect(java.util.stream.Collectors.joining("."));
        }
        String value = ss.value.getText();
        // Strip surrounding quotes if present
        if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
        }
        return new org.pangrova.parser.ast.SetSessionStatement(property, value);
    }

    @Override
    public Statement visitIncrementalReadStmt(PangrovaSqlParser.IncrementalReadStmtContext ctx) {
        var incr = ctx.incrementalReadStatement();
        List<SelectItem> selectItems = incr.selectItems().selectItem().stream()
                .map(si -> (SelectItem) visit(si)).toList();
        List<String> tableName = incr.qualifiedName().identifier().stream()
                .map(this::extractIdentifierText).toList();
        Expression fromSnapshot = (Expression) visit(incr.fromSnapshot);
        Expression toSnapshot = (Expression) visit(incr.toSnapshot);
        Optional<Expression> where = visitOptionalExpr(incr.where);
        List<SortItem> orderBy = incr.orderByList() != null
                ? incr.orderByList().sortItem().stream().map(si -> (SortItem) visit(si)).toList()
                : List.of();
        Optional<Expression> limit = visitOptionalExpr(incr.limit);
        return new IncrementalReadStatement(selectItems, tableName, fromSnapshot, toSnapshot, where, orderBy, limit);
    }

    @Override public MergeStatement.MergeClause visitMergeWhenMatched(PangrovaSqlParser.MergeWhenMatchedContext ctx) { return (MergeStatement.MergeClause) dml.visitMergeWhenMatched(ctx); }
    @Override public MergeStatement.MergeClause visitMergeWhenMatchedDelete(PangrovaSqlParser.MergeWhenMatchedDeleteContext ctx) { return (MergeStatement.MergeClause) dml.visitMergeWhenMatchedDelete(ctx); }
    @Override public MergeStatement.MergeClause visitMergeWhenNotMatched(PangrovaSqlParser.MergeWhenNotMatchedContext ctx) { return (MergeStatement.MergeClause) dml.visitMergeWhenNotMatched(ctx); }

    @Override
    public Statement visitSetQuery(PangrovaSqlParser.SetQueryContext ctx) {
        List<Query> queries = ctx.query().stream().map(this::visitQuery).toList();
        if (queries.size() == 1 && ctx.setOperator().isEmpty()) {
            return queries.get(0);
        }
        List<SetQuery.SetOperatorType> operators = ctx.setOperator().stream()
                .map(this::visitSetOperatorType).toList();
        return withLocation(new SetQuery(queries, operators), ctx);
    }
    private SetQuery.SetOperatorType visitSetOperatorType(PangrovaSqlParser.SetOperatorContext ctx) {
        if (ctx instanceof PangrovaSqlParser.UnionAllContext) return SetQuery.SetOperatorType.UNION_ALL;
        if (ctx instanceof PangrovaSqlParser.UnionDistinctContext) return SetQuery.SetOperatorType.UNION_DISTINCT;
        if (ctx instanceof PangrovaSqlParser.IntersectOpContext) return SetQuery.SetOperatorType.INTERSECT;
        if (ctx instanceof PangrovaSqlParser.ExceptOpContext) return SetQuery.SetOperatorType.EXCEPT;
        throw new IllegalArgumentException("Unknown set operator: " + ctx.getText());
    }
    @Override
    public Query visitQuery(PangrovaSqlParser.QueryContext ctx) {
        boolean distinct = ctx.distinct != null;
        List<SelectItem> selectItems = ctx.selectItems().selectItem().stream()
                .map(si -> (SelectItem) visit(si)).toList();
        var fromResult = parseTableRefList(ctx.tableRefList());
        Optional<Expression> where = visitOptionalExpr(ctx.where);
        List<GroupByItem> groupBy = convertGroupByClause(ctx.groupByClause());
        Optional<Expression> having = visitOptionalExpr(ctx.having);
        Optional<Expression> qualify = visitOptionalExpr(ctx.qualify);
        List<NamedWindow> windowDefs = ctx.windowDefinitionList() != null
                ? ctx.windowDefinitionList().windowDefinition().stream()
                        .map(this::buildNamedWindow).toList()
                : List.of();
        List<SortItem> orderBy = ctx.orderByList() != null
                ? ctx.orderByList().sortItem().stream().map(si -> (SortItem) visit(si)).toList()
                : List.of();
        Optional<RerankClause> rerankBy = ctx.rerankClause() != null
                ? Optional.of(buildRerankClause(ctx.rerankClause()))
                : Optional.empty();
        Optional<Expression> limit = visitOptionalExpr(ctx.limit);
        Optional<Expression> offset = visitOptionalExpr(ctx.offset);
        Query query = new Query(distinct, selectItems, fromResult.tables(), fromResult.joins(),
                where, groupBy, having, qualify, windowDefs, orderBy, rerankBy, limit, offset);
        return withLocation(query, ctx);
    }

    private NamedWindow buildNamedWindow(PangrovaSqlParser.WindowDefinitionContext ctx) {
        String name = extractIdentifierText(ctx.name);
        List<Expression> partitionBy = ctx.expressionList() != null
                ? toExpressionList(ctx.expressionList()) : List.of();
        List<SortItem> orderBy = ctx.orderByList() != null
                ? ctx.orderByList().sortItem().stream().map(si -> (SortItem) visit(si)).toList()
                : List.of();
        Optional<WindowSpec.WindowFrame> frame = ctx.windowFrame() != null
                ? Optional.of(expr.visitWindowFrameNode(ctx.windowFrame()))
                : Optional.empty();
        return new NamedWindow(name, partitionBy, orderBy, frame);
    }
    private record FromResult(List<TableRef> tables, List<JoinClause> joins) {
        static final FromResult EMPTY = new FromResult(List.of(), List.of());
    }
    /**
     * Parses the FROM clause table reference list into tables and joins.
     *
     * <p>The grammar enforces SQL standard precedence: explicit JOIN binds tighter
     * than comma. Each comma-separated element is a {@code joinedTable} that may
     * contain explicit JOINs internally. The first table of each comma-separated
     * group goes into {@code tables}; explicit JOINs within a group go into
     * {@code joins}. Cross-join semantics between comma-separated groups are
     * represented by adding each group's base table to the {@code tables} list.</p>
     */
    private FromResult parseTableRefList(PangrovaSqlParser.TableRefListContext ctx) {
        if (ctx == null) return FromResult.EMPTY;
        List<PangrovaSqlParser.JoinedTableContext> joinedTables = ctx.joinedTable();
        List<TableRef> tables = new java.util.ArrayList<>();
        List<JoinClause> joins = new java.util.ArrayList<>();
        for (PangrovaSqlParser.JoinedTableContext joinedTable : joinedTables) {
            List<PangrovaSqlParser.TableRefContext> tableRefs = joinedTable.tableRef();
            List<PangrovaSqlParser.JoinTypeContext> joinTypes = joinedTable.joinType();
            List<PangrovaSqlParser.JoinConditionContext> joinConditions = joinedTable.joinCondition();
            tables.add((TableRef) visit(tableRefs.get(0)));
            int condIdx = 0;
            for (int i = 1; i < tableRefs.size(); i++) {
                JoinClause.JoinType jt = resolveJoinType(joinTypes.get(i - 1));
                TableRef right = (TableRef) visit(tableRefs.get(i));
                if (condIdx < joinConditions.size()) {
                    var cond = joinConditions.get(condIdx);
                    condIdx++;
                    if (cond instanceof PangrovaSqlParser.JoinUsingConditionContext usingCtx) {
                        List<String> cols = usingCtx.columnNameList().identifier().stream()
                                .map(this::extractIdentifierText).toList();
                        joins.add(JoinClause.withUsing(jt, right, cols));
                    } else if (cond instanceof PangrovaSqlParser.JoinOnConditionContext onCtx) {
                        joins.add(new JoinClause(jt, right, Optional.of((Expression) visit(onCtx.expression()))));
                    }
                } else {
                    joins.add(new JoinClause(jt, right, Optional.empty()));
                }
            }
        }
        return new FromResult(tables, joins);
    }
    private static JoinClause.JoinType resolveJoinType(PangrovaSqlParser.JoinTypeContext ctx) {
        if (ctx.CROSS() != null) return JoinClause.JoinType.CROSS;
        if (ctx.LEFT() != null) return JoinClause.JoinType.LEFT;
        if (ctx.RIGHT() != null) return JoinClause.JoinType.RIGHT;
        if (ctx.FULL() != null) return JoinClause.JoinType.FULL;
        return JoinClause.JoinType.INNER;
    }
    @Override public SelectItem visitSelectAll(PangrovaSqlParser.SelectAllContext ctx) { return new AllColumns(); }

    @Override
    public SelectItem visitSelectExpression(PangrovaSqlParser.SelectExpressionContext ctx) {
        Expression expression = (Expression) visit(ctx.expression());
        Optional<String> alias = ctx.alias != null
                ? Optional.of(extractIdentifierText(ctx.alias)) : Optional.empty();
        return new ExpressionItem(expression, alias);
    }
    @Override
    public TableRef visitNamedTableRef(PangrovaSqlParser.NamedTableRefContext ctx) {
        Identifier name = visitQualifiedName(ctx.qualifiedName());
        Optional<TimeTravelSpec> timeTravel = ctx.timeTravelSpec() != null
                ? Optional.of((TimeTravelSpec) visit(ctx.timeTravelSpec())) : Optional.empty();
        Optional<String> alias = ctx.identifier() != null
                ? Optional.of(extractIdentifierText(ctx.identifier())) : Optional.empty();

        // Detect system table syntax: the last identifier part contains '$'
        // e.g., "orders$snapshots" or "catalog.schema.orders$files"
        List<String> parts = name.parts();
        String lastPart = parts.get(parts.size() - 1);
        int dollarIdx = lastPart.indexOf('$');
        var finalIdentifier = ctx.qualifiedName().identifier()
                .get(ctx.qualifiedName().identifier().size() - 1);
        if (dollarIdx >= 0 && (finalIdentifier.QUOTED_IDENTIFIER() != null
                || finalIdentifier.BACKTICK_IDENTIFIER() != null)) {
            throw new ParseException(
                    org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    "Quoted table names containing '$' are not supported; use an unquoted system-table suffix",
                    finalIdentifier.getStart().getLine(),
                    finalIdentifier.getStart().getCharPositionInLine());
        }
        if (dollarIdx > 0 && dollarIdx < lastPart.length() - 1) {
            String baseTablePart = lastPart.substring(0, dollarIdx);
            String systemTableName = lastPart.substring(dollarIdx + 1);
            List<String> baseParts = new java.util.ArrayList<>(parts.subList(0, parts.size() - 1));
            baseParts.add(baseTablePart);
            Identifier baseTable = new Identifier(baseParts);
            var sysRef = new SystemTableReference(baseTable, systemTableName);
            return TableRef.systemTable(sysRef, timeTravel, alias);
        }

        if (ctx.tableSampleClause() != null) {
            TableSample sample = buildTableSample(ctx.tableSampleClause());
            return TableRef.sampled(name, timeTravel, alias, sample);
        }

        return new TableRef(name, timeTravel, alias);
    }

    private TableSample buildTableSample(PangrovaSqlParser.TableSampleClauseContext ctx) {
        TableSample.SamplingMethod method = ctx.sampleMethod().BERNOULLI() != null
                ? TableSample.SamplingMethod.BERNOULLI
                : TableSample.SamplingMethod.SYSTEM;
        Expression percentage = (Expression) visit(ctx.percentage);
        Optional<Expression> seed = ctx.seed != null
                ? Optional.of((Expression) visit(ctx.seed))
                : Optional.empty();
        return new TableSample(method, percentage, seed);
    }
    @Override
    public TableRef visitDerivedTableRef(PangrovaSqlParser.DerivedTableRefContext ctx) {
        Statement subquery = (Statement) visit(ctx.setQuery());
        Optional<String> alias = ctx.identifier() != null
                ? Optional.of(extractIdentifierText(ctx.identifier())) : Optional.empty();
        return TableRef.derived(subquery, alias);
    }
    @Override
    public TableRef visitValuesTableRef(PangrovaSqlParser.ValuesTableRefContext ctx) {
        List<String> columns = ctx.columnNameList() == null ? List.of()
                : ctx.columnNameList().identifier().stream()
                        .map(this::extractIdentifierText).toList();
        List<List<Expression>> rows = ctx.valuesList().valueTuple().stream()
                .map(tuple -> tuple.expression().stream()
                        .map(value -> (Expression) visit(value)).toList())
                .toList();
        Optional<String> alias = ctx.alias == null
                ? Optional.empty() : Optional.of(extractIdentifierText(ctx.alias));
        return TableRef.values(new ValuesTable(rows, columns), alias);
    }
    @Override
    public TableRef visitLateralTableFunctionRef(PangrovaSqlParser.LateralTableFunctionRefContext ctx) {
        var funcRef = buildTableFunctionRef(ctx.tableFunctionCall(),
                ctx.identifier() != null ? Optional.of(extractIdentifierText(ctx.identifier())) : Optional.empty(),
                ctx.columnNameList() != null
                        ? ctx.columnNameList().identifier().stream().map(this::extractIdentifierText).toList()
                        : List.of());
        return TableRef.lateralTableFunction(funcRef);
    }
    @Override
    public TableRef visitStandaloneTableFunctionRef(PangrovaSqlParser.StandaloneTableFunctionRefContext ctx) {
        Optional<String> alias = Optional.empty();
        List<String> columnAliases = List.of();
        if (ctx.identifier() != null) {
            alias = Optional.of(extractIdentifierText(ctx.identifier()));
            if (ctx.columnNameList() != null) {
                columnAliases = ctx.columnNameList().identifier().stream()
                        .map(this::extractIdentifierText).toList();
            }
        }
        var funcRef = buildTableFunctionRef(ctx.tableFunctionCall(), alias, columnAliases);
        return TableRef.lateralTableFunction(funcRef);
    }
    @Override
    public TableRef visitDirectGenerateSeriesRef(PangrovaSqlParser.DirectGenerateSeriesRefContext ctx) {
        Optional<String> alias = ctx.alias == null
                ? Optional.empty() : Optional.of(extractIdentifierText(ctx.alias));
        List<String> columnAliases = ctx.columnNameList() == null ? List.of()
                : ctx.columnNameList().identifier().stream().map(this::extractIdentifierText).toList();
        return TableRef.lateralTableFunction(new TableFunctionRef(
                TableFunctionRef.TableFunctionType.GENERATE_SERIES,
                (Expression) visit(ctx.rangeStart), Optional.of((Expression) visit(ctx.rangeEnd)),
                ctx.rangeStep == null ? Optional.empty() : Optional.of((Expression) visit(ctx.rangeStep)),
                alias, columnAliases));
    }
    private TableFunctionRef buildTableFunctionRef(PangrovaSqlParser.TableFunctionCallContext funcCall,
                                                   Optional<String> alias, List<String> columnAliases) {
        if (funcCall.GENERATE_SERIES() != null) {
            Expression startExpr = (Expression) visit(funcCall.rangeStart);
            Expression endExpr = (Expression) visit(funcCall.rangeEnd);
            return new TableFunctionRef(TableFunctionRef.TableFunctionType.GENERATE_SERIES,
                    startExpr, Optional.of(endExpr),
                    funcCall.rangeStep == null ? Optional.empty()
                            : Optional.of((Expression) visit(funcCall.rangeStep)), alias, columnAliases);
        }
        TableFunctionRef.TableFunctionType funcType = funcCall.EXPLODE() != null
                ? TableFunctionRef.TableFunctionType.EXPLODE
                : TableFunctionRef.TableFunctionType.POSEXPLODE;
        Expression argument = (Expression) visit(funcCall.expression(0));
        return new TableFunctionRef(funcType, argument, alias, columnAliases);
    }
    @Override
    public TableRef visitLateralExplodeRef(PangrovaSqlParser.LateralExplodeRefContext ctx) {
        Expression argument = (Expression) visit(ctx.expression());
        Optional<String> alias = ctx.identifier() != null
                ? Optional.of(extractIdentifierText(ctx.identifier())) : Optional.empty();
        List<String> columnAliases = ctx.columnNameList() != null
                ? ctx.columnNameList().identifier().stream().map(this::extractIdentifierText).toList()
                : List.of();
        var funcRef = new TableFunctionRef(TableFunctionRef.TableFunctionType.EXPLODE, argument, alias, columnAliases);
        return TableRef.lateralTableFunction(funcRef);
    }
    @Override
    public TableRef visitLateralPosexplodeRef(PangrovaSqlParser.LateralPosexplodeRefContext ctx) {
        Expression argument = (Expression) visit(ctx.expression());
        Optional<String> alias = ctx.identifier() != null
                ? Optional.of(extractIdentifierText(ctx.identifier())) : Optional.empty();
        List<String> columnAliases = ctx.columnNameList() != null
                ? ctx.columnNameList().identifier().stream().map(this::extractIdentifierText).toList()
                : List.of();
        var funcRef = new TableFunctionRef(TableFunctionRef.TableFunctionType.POSEXPLODE, argument, alias, columnAliases);
        return TableRef.lateralTableFunction(funcRef);
    }
    @Override
    public TableRef visitUnnestTableRef(PangrovaSqlParser.UnnestTableRefContext ctx) {
        Expression argument = (Expression) visit(ctx.expression());
        Optional<String> alias = ctx.alias == null
                ? Optional.empty() : Optional.of(extractIdentifierText(ctx.alias));
        List<String> columnAliases = ctx.columnNameList() == null
                ? List.of() : ctx.columnNameList().identifier().stream()
                        .map(this::extractIdentifierText).toList();
        var kind = ctx.ORDINALITY() == null
                ? TableFunctionRef.TableFunctionType.UNNEST
                : TableFunctionRef.TableFunctionType.UNNEST_WITH_ORDINALITY;
        return TableRef.lateralTableFunction(
                new TableFunctionRef(kind, argument, alias, columnAliases));
    }
    @Override
    public TableRef visitLateralDerivedTableRef(PangrovaSqlParser.LateralDerivedTableRefContext ctx) {
        Statement subquery = (Statement) visit(ctx.setQuery());
        Optional<String> alias = ctx.identifier() != null
                ? Optional.of(extractIdentifierText(ctx.identifier())) : Optional.empty();
        return TableRef.lateralDerived(subquery, alias);
    }
    @Override
    public TableRef visitPivotTableRef(PangrovaSqlParser.PivotTableRefContext ctx) {
        TableRef source = (TableRef) visit(ctx.tableRef());
        List<PivotClause.PivotAggregate> aggregates = ctx.pivotAggregateList().pivotAggregate().stream()
                .map(this::buildPivotAggregate).toList();
        // The FOR column is the first identifier in the list (alias is labeled separately)
        List<PangrovaSqlParser.IdentifierContext> allIds = ctx.identifier();
        PangrovaSqlParser.IdentifierContext forColumnCtx = allIds.stream()
                .filter(id -> id != ctx.alias).findFirst().orElseThrow();
        Identifier forColumn = new Identifier(List.of(extractIdentifierText(forColumnCtx)));
        List<PivotClause.PivotValue> inValues = ctx.pivotValueList().pivotValue().stream()
                .map(this::buildPivotValue).toList();
        Optional<String> alias = ctx.alias != null
                ? Optional.of(extractIdentifierText(ctx.alias)) : Optional.empty();
        var pivot = new PivotClause(aggregates, forColumn, inValues);
        return TableRef.pivot(source, pivot, alias);
    }

    private PivotClause.PivotAggregate buildPivotAggregate(PangrovaSqlParser.PivotAggregateContext ctx) {
        Identifier function = visitQualifiedName(ctx.qualifiedName());
        Expression argument = (Expression) visit(ctx.expression());
        Optional<String> alias = ctx.alias != null
                ? Optional.of(extractIdentifierText(ctx.alias)) : Optional.empty();
        return new PivotClause.PivotAggregate(function, argument, alias);
    }

    private PivotClause.PivotValue buildPivotValue(PangrovaSqlParser.PivotValueContext ctx) {
        Expression value = (Expression) visit(ctx.expression());
        Optional<String> alias = ctx.alias != null
                ? Optional.of(extractIdentifierText(ctx.alias)) : Optional.empty();
        return new PivotClause.PivotValue(value, alias);
    }

    @Override
    public TableRef visitUnpivotTableRef(PangrovaSqlParser.UnpivotTableRefContext ctx) {
        TableRef source = (TableRef) visit(ctx.tableRef());
        List<PangrovaSqlParser.IdentifierContext> identifiers = ctx.identifier().stream()
                .filter(id -> id != ctx.alias).toList();
        Identifier valueColumn = new Identifier(List.of(extractIdentifierText(identifiers.get(0))));
        Identifier nameColumn = new Identifier(List.of(extractIdentifierText(identifiers.get(1))));
        UnpivotClause.NullHandling nullHandling = UnpivotClause.NullHandling.EXCLUDE;
        if (ctx.nullsOption() != null && ctx.nullsOption().INCLUDE() != null) {
            nullHandling = UnpivotClause.NullHandling.INCLUDE;
        }
        List<UnpivotClause.UnpivotColumn> sourceColumns = ctx.unpivotColumnList().unpivotColumn().stream()
                .map(this::buildUnpivotColumn).toList();
        Optional<String> alias = ctx.alias != null
                ? Optional.of(extractIdentifierText(ctx.alias)) : Optional.empty();
        var unpivot = new UnpivotClause(valueColumn, nameColumn, sourceColumns, nullHandling);
        return TableRef.unpivot(source, unpivot, alias);
    }

    private UnpivotClause.UnpivotColumn buildUnpivotColumn(PangrovaSqlParser.UnpivotColumnContext ctx) {
        Identifier column = new Identifier(List.of(extractIdentifierText(ctx.identifier())));
        Optional<String> alias = ctx.alias != null
                ? Optional.of(stripQuotes(ctx.alias.getText())) : Optional.empty();
        return new UnpivotClause.UnpivotColumn(column, alias);
    }
    @Override public TimeTravelSpec visitSystemTimeTravel(PangrovaSqlParser.SystemTimeTravelContext ctx) { return new TimeTravelSpec(Type.SYSTEM_TIME, (Expression) visit(ctx.expression())); }
    @Override public TimeTravelSpec visitSystemVersionTravel(PangrovaSqlParser.SystemVersionTravelContext ctx) { return new TimeTravelSpec(Type.SYSTEM_VERSION, (Expression) visit(ctx.expression())); }
    @Override public TimeTravelSpec visitBranchVersionTravel(PangrovaSqlParser.BranchVersionTravelContext ctx) { return TimeTravelSpec.branch(stripQuotes(ctx.STRING_LITERAL().getText())); }
    @Override public TimeTravelSpec visitTagVersionTravel(PangrovaSqlParser.TagVersionTravelContext ctx) { return TimeTravelSpec.tag(stripQuotes(ctx.STRING_LITERAL().getText())); }
    @Override public SortItem visitSortItem(PangrovaSqlParser.SortItemContext ctx) { return expr.visitSortItem(ctx); }
    @Override public Expression visitPrimaryExpr(PangrovaSqlParser.PrimaryExprContext ctx) { return expr.visitPrimaryExpr(ctx); }
    @Override public Expression visitSubscriptExpr(PangrovaSqlParser.SubscriptExprContext ctx) { return expr.visitSubscriptExpr(ctx); }
    @Override public Expression visitMultiplicativeExpr(PangrovaSqlParser.MultiplicativeExprContext ctx) { return expr.visitMultiplicativeExpr(ctx); }
    @Override public Expression visitAdditiveExpr(PangrovaSqlParser.AdditiveExprContext ctx) { return expr.visitAdditiveExpr(ctx); }
    @Override public Expression visitConcatExpr(PangrovaSqlParser.ConcatExprContext ctx) { return expr.visitConcatExpr(ctx); }
    @Override public Expression visitComparisonExpr(PangrovaSqlParser.ComparisonExprContext ctx) { return expr.visitComparisonExpr(ctx); }
    @Override public Expression visitAndExpr(PangrovaSqlParser.AndExprContext ctx) { return expr.visitAndExpr(ctx); }
    @Override public Expression visitOrExpr(PangrovaSqlParser.OrExprContext ctx) { return expr.visitOrExpr(ctx); }
    @Override public Expression visitNotExpr(PangrovaSqlParser.NotExprContext ctx) { return expr.visitNotExpr(ctx); }
    @Override public Expression visitUnaryMinusExpr(PangrovaSqlParser.UnaryMinusExprContext ctx) { return expr.visitUnaryMinusExpr(ctx); }
    @Override public Expression visitInSubqueryExpr(PangrovaSqlParser.InSubqueryExprContext ctx) { return expr.visitInSubqueryExpr(ctx); }
    @Override public Expression visitInListExpr(PangrovaSqlParser.InListExprContext ctx) { return expr.visitInListExpr(ctx); }
    @Override public Expression visitNotInSubqueryExpr(PangrovaSqlParser.NotInSubqueryExprContext ctx) { return expr.visitNotInSubqueryExpr(ctx); }
    @Override public Expression visitNotInListExpr(PangrovaSqlParser.NotInListExprContext ctx) { return expr.visitNotInListExpr(ctx); }
    @Override public Expression visitParenExpr(PangrovaSqlParser.ParenExprContext ctx) { return expr.visitParenExpr(ctx); }
    @Override public Expression visitIsNullExpr(PangrovaSqlParser.IsNullExprContext ctx) { return expr.visitIsNullExpr(ctx); }
    @Override public Expression visitTruthTestExpr(PangrovaSqlParser.TruthTestExprContext ctx) { return expr.visitTruthTestExpr(ctx); }
    @Override public Expression visitBetweenExpr(PangrovaSqlParser.BetweenExprContext ctx) { return expr.visitBetweenExpr(ctx); }
    @Override public Expression visitLikeExpr(PangrovaSqlParser.LikeExprContext ctx) { return expr.visitLikeExpr(ctx); }
    @Override public Expression visitExistsExpr(PangrovaSqlParser.ExistsExprContext ctx) { return expr.visitExistsExpr(ctx); }
    @Override public Expression visitSimpleCaseExpr(PangrovaSqlParser.SimpleCaseExprContext ctx) { return expr.visitSimpleCaseExpr(ctx); }
    @Override public Expression visitSearchedCaseExpr(PangrovaSqlParser.SearchedCaseExprContext ctx) { return expr.visitSearchedCaseExpr(ctx); }
    @Override public Expression visitLiteralExpr(PangrovaSqlParser.LiteralExprContext ctx) { return expr.visitLiteralExpr(ctx); }
    @Override public Expression visitCastExpr(PangrovaSqlParser.CastExprContext ctx) { return expr.visitCastExpr(ctx); }
    @Override public Expression visitIdentifierExpr(PangrovaSqlParser.IdentifierExprContext ctx) { return expr.visitIdentifierExpr(ctx); }
    @Override public Expression visitFunctionCallExpr(PangrovaSqlParser.FunctionCallExprContext ctx) { return expr.visitFunctionCallExpr(ctx); }
    @Override public Expression visitStarFunctionCallExpr(PangrovaSqlParser.StarFunctionCallExprContext ctx) { return expr.visitStarFunctionCallExpr(ctx); }
    @Override public Expression visitScalarSubqueryExpr(PangrovaSqlParser.ScalarSubqueryExprContext ctx) { return expr.visitScalarSubqueryExpr(ctx); }
    @Override public Expression visitArrayConstructorExpr(PangrovaSqlParser.ArrayConstructorExprContext ctx) { return expr.visitArrayConstructorExpr(ctx); }
    @Override public Expression visitParameterMarkerExpr(PangrovaSqlParser.ParameterMarkerExprContext ctx) { return expr.visitParameterMarkerExpr(ctx); }
    @Override public Literal visitIntegerLiteral(PangrovaSqlParser.IntegerLiteralContext ctx) { return expr.visitIntegerLiteral(ctx); }
    @Override public Literal visitDecimalLiteral(PangrovaSqlParser.DecimalLiteralContext ctx) { return expr.visitDecimalLiteral(ctx); }
    @Override public Literal visitStringLiteral(PangrovaSqlParser.StringLiteralContext ctx) { return expr.visitStringLiteral(ctx); }
    @Override public Literal visitBooleanTrue(PangrovaSqlParser.BooleanTrueContext ctx) { return expr.visitBooleanTrue(ctx); }
    @Override public Literal visitBooleanFalse(PangrovaSqlParser.BooleanFalseContext ctx) { return expr.visitBooleanFalse(ctx); }
    @Override public Literal visitNullLiteral(PangrovaSqlParser.NullLiteralContext ctx) { return expr.visitNullLiteral(ctx); }
    @Override public Literal visitDateLiteral(PangrovaSqlParser.DateLiteralContext ctx) { return expr.visitDateLiteral(ctx); }
    @Override public Literal visitUuidLiteral(PangrovaSqlParser.UuidLiteralContext ctx) { return expr.visitUuidLiteral(ctx); }
    @Override public Literal visitTimestampLiteral(PangrovaSqlParser.TimestampLiteralContext ctx) { return expr.visitTimestampLiteral(ctx); }
    @Override public Literal visitIntervalLiteral(PangrovaSqlParser.IntervalLiteralContext ctx) { return expr.visitIntervalLiteral(ctx); }
    @Override public Literal visitBinaryHexLiteral(PangrovaSqlParser.BinaryHexLiteralContext ctx) { return expr.visitBinaryHexLiteral(ctx); }
    @Override public Literal visitTimestampWithTimeZoneLiteral(PangrovaSqlParser.TimestampWithTimeZoneLiteralContext ctx) { return expr.visitTimestampWithTimeZoneLiteral(ctx); }
    @Override public Expression visitExtractExpr(PangrovaSqlParser.ExtractExprContext ctx) { return expr.visitExtractExpr(ctx); }
    @Override public Expression visitSubstringFromForExpr(PangrovaSqlParser.SubstringFromForExprContext ctx) { return expr.visitSubstringFromForExpr(ctx); }
    @Override
    public Identifier visitQualifiedName(PangrovaSqlParser.QualifiedNameContext ctx) {
        List<String> parts = ctx.identifier().stream().map(this::extractIdentifierText).toList();
        return new Identifier(parts);
    }
    Optional<Expression> visitOptionalExpr(PangrovaSqlParser.ExpressionContext ctx) {
        return ctx != null ? Optional.of((Expression) visit(ctx)) : Optional.empty();
    }
    List<Expression> toExpressionList(PangrovaSqlParser.ExpressionListContext ctx) {
        if (ctx == null) return List.of();
        return ctx.expression().stream().map(e -> (Expression) visit(e)).toList();
    }
    String extractIdentifierText(PangrovaSqlParser.IdentifierContext ctx) {
        if (ctx.QUOTED_IDENTIFIER() != null) {
            // Double-quoted identifier: reject if dialect does not allow double-quotes
            if (!dialect.allowsDoubleQuotes()) {
                throw new ParseException(
                        org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                        "Double-quoted identifiers are not allowed in " + dialect.name() + " dialect",
                        ctx.getStart().getLine(),
                        ctx.getStart().getCharPositionInLine());
            }
            String raw = ctx.QUOTED_IDENTIFIER().getText();
            // Quoted identifiers always preserve case
            return raw.substring(1, raw.length() - 1).replace("\"\"", "\"");
        }
        if (ctx.BACKTICK_IDENTIFIER() != null) {
            // Backtick-quoted identifier: reject if dialect does not allow backticks
            if (!dialect.allowsBackticks()) {
                throw new ParseException(
                        org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                        "Backtick-quoted identifiers are not allowed in " + dialect.name() + " dialect",
                        ctx.getStart().getLine(),
                        ctx.getStart().getCharPositionInLine());
            }
            String raw = ctx.BACKTICK_IDENTIFIER().getText();
            // Quoted identifiers always preserve case; strip delimiters and unescape
            return raw.substring(1, raw.length() - 1).replace("``", "`");
        }
        if (ctx.DOLLAR_IDENTIFIER() != null) {
            return dialect.foldIdentifier(ctx.DOLLAR_IDENTIFIER().getText());
        }
        // Unquoted identifier or keyword: apply dialect case folding
        return dialect.foldIdentifier(ctx.getText());
    }

    static String stripQuotes(String s) {
        if (s.length() >= 2 && s.startsWith("'") && s.endsWith("'")) {
            return s.substring(1, s.length() - 1).replace("''", "'");
        }
        return s;
    }

    private List<GroupByItem> convertGroupByClause(PangrovaSqlParser.GroupByClauseContext ctx) {
        if (ctx == null) {
            return List.of();
        }
        return ctx.groupByElement().stream()
                .map(this::convertGroupByElement)
                .toList();
    }
    private GroupByItem convertGroupByElement(PangrovaSqlParser.GroupByElementContext ctx) {
        if (ctx instanceof PangrovaSqlParser.GroupingSetsElementContext gs) {
            List<List<Expression>> sets = gs.groupingSet().stream()
                    .map(this::convertGroupingSet)
                    .toList();
            return new GroupingSets(sets);
        }
        if (ctx instanceof PangrovaSqlParser.CubeElementContext cube) {
            List<Expression> columns = cube.expressionList().expression().stream()
                    .map(e -> (Expression) visit(e))
                    .toList();
            return new CubeGroupBy(columns);
        }
        if (ctx instanceof PangrovaSqlParser.RollupElementContext rollup) {
            List<Expression> columns = rollup.expressionList().expression().stream()
                    .map(e -> (Expression) visit(e))
                    .toList();
            return new RollupGroupBy(columns);
        }
        if (ctx instanceof PangrovaSqlParser.GroupByExpressionContext groupByExpression) {
            return new GroupByExpression((Expression) visit(groupByExpression.expression()));
        }
        throw new IllegalArgumentException("Unknown GROUP BY element: " + ctx.getText());
    }
    private List<Expression> convertGroupingSet(PangrovaSqlParser.GroupingSetContext ctx) {
        if (ctx.expressionList() != null) {
            return ctx.expressionList().expression().stream()
                    .map(e -> (Expression) visit(e))
                    .toList();
        }
        if (ctx.expression() != null) {
            return List.of((Expression) visit(ctx.expression()));
        }
        return List.of();
    }
    private RerankClause buildRerankClause(PangrovaSqlParser.RerankClauseContext ctx) {
        RerankStrategy strategy = buildRerankStrategy(ctx.rerankStrategy());
        List<Identifier> onColumns = ctx.rerankColumnList() != null
                ? ctx.rerankColumnList().identifier().stream()
                    .map(id -> Identifier.of(extractIdentifierText(id)))
                    .toList()
                : List.of();
        return new RerankClause(strategy, onColumns);
    }
    private RerankStrategy buildRerankStrategy(PangrovaSqlParser.RerankStrategyContext ctx) {
        if (ctx instanceof PangrovaSqlParser.RrfStrategyContext rrf) {
            return new RrfStrategy((Expression) visit(rrf.expression()));
        }
        if (ctx instanceof PangrovaSqlParser.LinearCombinationStrategyContext lc) {
            var exprs = lc.expression();
            return new LinearCombinationStrategy(
                    (Expression) visit(exprs.get(0)),
                    (Expression) visit(exprs.get(1)));
        }
        if (ctx instanceof PangrovaSqlParser.CrossEncoderStrategyContext ce) {
            String modelName = stripQuotes(ce.STRING_LITERAL().getText());
            return new CrossEncoderStrategy(modelName, (Expression) visit(ce.expression()));
        }
        if (ctx instanceof PangrovaSqlParser.CascadeStrategyContext cascade) {
            List<RerankStrategy> stages = cascade.rerankStrategy().stream()
                    .map(this::buildRerankStrategy)
                    .toList();
            return new CascadeStrategy(stages);
        }
        throw new IllegalArgumentException("Unknown rerank strategy: " + ctx.getText());
    }
}
