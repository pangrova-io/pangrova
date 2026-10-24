/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.parser;

import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.statement.*;
import org.pangrova.parser.ast.statement.stream.AlterStreamAction;
import org.pangrova.parser.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.ast.statement.stream.DescribeStreamStatement;
import org.pangrova.parser.ast.statement.stream.DropStreamStatement;
import org.pangrova.parser.ast.statement.stream.ShowStreamsStatement;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Handles extended DDL visitor methods: dynamic tables, indexes, branches/tags,
 * clustering, streams, embedding models, and backfill operations.
 * Extracted from AstDdlDmlBuilder to keep each class under the 500-line limit.
 */
final class AstExtendedDdlBuilder extends PangrovaSqlBaseVisitor<Object> {

    private final AstBuilder parent;
    private final AstVersionedTableDdlBuilder versionedTableDdl;

    AstExtendedDdlBuilder(AstBuilder parent) {
        this.parent = parent;
        this.versionedTableDdl = new AstVersionedTableDdlBuilder(parent);
    }

    @Override
    public CreateDynamicTableStatement visitCreateDynamicTable(
            PangrovaSqlParser.CreateDynamicTableContext ctx) {
        Identifier name = parent.visitQualifiedName(ctx.qualifiedName());
        Query query = parent.visitQuery(ctx.query());
        Duration targetLag = ctx.TARGET_LAG() != null
                ? parsePositiveTargetLag(ctx.STRING_LITERAL(0))
                : Duration.ZERO;
        Optional<RefreshMode> refreshMode = ctx.dtRefreshMode() != null
                ? Optional.of(visitDtRefreshMode(ctx.dtRefreshMode()))
                : Optional.empty();
        Optional<String> warehouse = ctx.WAREHOUSE() != null
                ? Optional.of(AstDdlDmlBuilder.stripQuotes(ctx.STRING_LITERAL(
                        ctx.STRING_LITERAL().size() - 1).getText()))
                : Optional.empty();
        Map<String, String> properties = ctx.stringPropertyList() != null
                ? visitDtProperties(ctx.stringPropertyList())
                : Map.of();
        return new CreateDynamicTableStatement(name, query, targetLag, refreshMode, warehouse,
                properties, incrementalStrategy(ctx.incrementalStrategy()));
    }

    @Override
    public AlterDynamicTableStatement visitAlterDtSuspend(
            PangrovaSqlParser.AlterDtSuspendContext ctx) {
        return new AlterDynamicTableStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterDynamicTableAction.Suspend());
    }
    @Override
    public AlterDynamicTableStatement visitAlterDtResume(
            PangrovaSqlParser.AlterDtResumeContext ctx) {
        return new AlterDynamicTableStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterDynamicTableAction.Resume());
    }
    @Override
    public AlterDynamicTableStatement visitAlterDtRefresh(
            PangrovaSqlParser.AlterDtRefreshContext ctx) {
        return new AlterDynamicTableStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterDynamicTableAction.Refresh());
    }

    @Override
    public AlterDynamicTableStatement visitAlterDtSetTargetLag(
            PangrovaSqlParser.AlterDtSetTargetLagContext ctx) {
        Duration newLag = parsePositiveTargetLag(ctx.STRING_LITERAL());
        return new AlterDynamicTableStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterDynamicTableAction.SetTargetLag(newLag));
    }

    @Override
    public AlterDynamicTableStatement visitAlterDtSetRefreshMode(
            PangrovaSqlParser.AlterDtSetRefreshModeContext ctx) {
        return new AlterDynamicTableStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterDynamicTableAction.SetRefreshMode(visitDtRefreshMode(ctx.dtRefreshMode())));
    }

    @Override
    public AlterDynamicTableStatement visitAlterDtRename(
            PangrovaSqlParser.AlterDtRenameContext ctx) {
        return new AlterDynamicTableStatement(
                parent.visitQualifiedName(ctx.qualifiedName(0)),
                new AlterDynamicTableAction.Rename(parent.visitQualifiedName(ctx.qualifiedName(1))));
    }

    @Override
    public AlterDynamicTableStatement visitAlterDtRedefine(
            PangrovaSqlParser.AlterDtRedefineContext ctx) {
        Query newQuery = parent.visitQuery(ctx.query());
        return new AlterDynamicTableStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterDynamicTableAction.Redefine(newQuery));
    }

    @Override
    public AlterDynamicTableStatement visitAlterDtBackfillEmbedding(
            PangrovaSqlParser.AlterDtBackfillEmbeddingContext ctx) {
        String columnName = AstDdlDmlBuilder.extractIdentifierText(ctx.columnName);
        return new AlterDynamicTableStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterDynamicTableAction.BackfillEmbedding(columnName));
    }

    @Override
    public DropDynamicTableStatement visitDropDynamicTable(
            PangrovaSqlParser.DropDynamicTableContext ctx) {
        Identifier name = parent.visitQualifiedName(ctx.qualifiedName());
        boolean cascade = ctx.CASCADE() != null;
        return new DropDynamicTableStatement(name, cascade);
    }

    @Override
    public ShowDynamicTablesStatement visitShowDynamicTables(
            PangrovaSqlParser.ShowDynamicTablesContext ctx) {
        return new ShowDynamicTablesStatement();
    }

    @Override
    public DescribeDynamicTableStatement visitDescribeDynamicTable(
            PangrovaSqlParser.DescribeDynamicTableContext ctx) {
        return new DescribeDynamicTableStatement(
                parent.visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public CreateMaterializedViewStatement visitCreateMaterializedView(
            PangrovaSqlParser.CreateMaterializedViewContext ctx) {
        Identifier name = parent.visitQualifiedName(ctx.qualifiedName());
        Query query = parent.visitQuery(ctx.query());
        Duration targetLag = ctx.TARGET_LAG() != null
                ? parsePositiveTargetLag(ctx.STRING_LITERAL())
                : Duration.ZERO;
        Optional<RefreshMode> refreshMode = ctx.dtRefreshMode() != null
                ? Optional.of(visitDtRefreshMode(ctx.dtRefreshMode()))
                : Optional.empty();
        return new CreateMaterializedViewStatement(name, query, targetLag, refreshMode,
                ctx.REPLACE() != null, ctx.EXISTS() != null,
                incrementalStrategy(ctx.incrementalStrategy()));
    }

    @Override
    public AlterMaterializedViewStatement visitAlterMvSuspend(
            PangrovaSqlParser.AlterMvSuspendContext ctx) {
        return new AlterMaterializedViewStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterMaterializedViewAction.Suspend());
    }

    @Override
    public AlterMaterializedViewStatement visitAlterMvResume(
            PangrovaSqlParser.AlterMvResumeContext ctx) {
        return new AlterMaterializedViewStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterMaterializedViewAction.Resume());
    }

    @Override
    public AlterMaterializedViewStatement visitAlterMvRefresh(
            PangrovaSqlParser.AlterMvRefreshContext ctx) {
        return new AlterMaterializedViewStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterMaterializedViewAction.Refresh());
    }

    @Override
    public AlterMaterializedViewStatement visitAlterMvSetTargetLag(
            PangrovaSqlParser.AlterMvSetTargetLagContext ctx) {
        return new AlterMaterializedViewStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterMaterializedViewAction.SetTargetLag(
                        parsePositiveTargetLag(ctx.STRING_LITERAL())));
    }

    @Override
    public AlterMaterializedViewStatement visitAlterMvSetRefreshMode(
            PangrovaSqlParser.AlterMvSetRefreshModeContext ctx) {
        return new AlterMaterializedViewStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                new AlterMaterializedViewAction.SetRefreshMode(
                        visitDtRefreshMode(ctx.dtRefreshMode())));
    }

    @Override
    public AlterMaterializedViewStatement visitAlterMvRename(
            PangrovaSqlParser.AlterMvRenameContext ctx) {
        return new AlterMaterializedViewStatement(
                parent.visitQualifiedName(ctx.qualifiedName(0)),
                new AlterMaterializedViewAction.Rename(
                        parent.visitQualifiedName(ctx.qualifiedName(1))));
    }

    @Override
    public RefreshMaterializedViewStatement visitRefreshMaterializedView(
            PangrovaSqlParser.RefreshMaterializedViewContext ctx) {
        return new RefreshMaterializedViewStatement(
                parent.visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public DropMaterializedViewStatement visitDropMaterializedView(
            PangrovaSqlParser.DropMaterializedViewContext ctx) {
        return new DropMaterializedViewStatement(
                parent.visitQualifiedName(ctx.qualifiedName()),
                ctx.CASCADE() != null,
                ctx.EXISTS() != null);
    }

    @Override
    public ShowMaterializedViewsStatement visitShowMaterializedViews(
            PangrovaSqlParser.ShowMaterializedViewsContext ctx) {
        return new ShowMaterializedViewsStatement(ctx.STRING_LITERAL() == null ? null
                : AstDdlDmlBuilder.stripQuotes(ctx.STRING_LITERAL().getText()));
    }

    @Override
    public DescribeMaterializedViewStatement visitDescribeMaterializedView(
            PangrovaSqlParser.DescribeMaterializedViewContext ctx) {
        return new DescribeMaterializedViewStatement(
                parent.visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public ShowMaterializationsStatement visitShowMaterializations(
            PangrovaSqlParser.ShowMaterializationsContext ctx) {
        return new ShowMaterializationsStatement();
    }

    @Override
    public ShowMaterializationRefreshHistoryStatement visitShowMaterializationRefreshHistory(
            PangrovaSqlParser.ShowMaterializationRefreshHistoryContext ctx) {
        return new ShowMaterializationRefreshHistoryStatement(optionalName(ctx.qualifiedName()));
    }

    @Override
    public ShowMaterializationDependenciesStatement visitShowMaterializationDependencies(
            PangrovaSqlParser.ShowMaterializationDependenciesContext ctx) {
        return new ShowMaterializationDependenciesStatement(optionalName(ctx.qualifiedName()));
    }

    @Override
    public ShowMaterializationGcHistoryStatement visitShowMaterializationGcHistory(
            PangrovaSqlParser.ShowMaterializationGcHistoryContext ctx) {
        return new ShowMaterializationGcHistoryStatement(optionalName(ctx.qualifiedName()));
    }

    private Optional<Identifier> optionalName(PangrovaSqlParser.QualifiedNameContext ctx) {
        return Optional.ofNullable(ctx).map(parent::visitQualifiedName);
    }

    @Override
    public AlterTableStatement visitAlterTableCreateBranch(
            PangrovaSqlParser.AlterTableCreateBranchContext ctx) {
        return versionedTableDdl.visitAlterTableCreateBranch(ctx);
    }

    @Override
    public AlterTableStatement visitAlterTableDropBranch(
            PangrovaSqlParser.AlterTableDropBranchContext ctx) {
        return versionedTableDdl.visitAlterTableDropBranch(ctx);
    }

    @Override
    public AlterTableStatement visitAlterTableCreateTag(
            PangrovaSqlParser.AlterTableCreateTagContext ctx) {
        return versionedTableDdl.visitAlterTableCreateTag(ctx);
    }

    @Override
    public AlterTableStatement visitAlterTableDropTag(
            PangrovaSqlParser.AlterTableDropTagContext ctx) {
        return versionedTableDdl.visitAlterTableDropTag(ctx);
    }

    @Override
    public AlterTableStatement visitAlterTableSetColumnTag(
            PangrovaSqlParser.AlterTableSetColumnTagContext ctx) {
        return versionedTableDdl.visitAlterTableSetColumnTag(ctx);
    }

    @Override
    public AlterTableStatement visitAlterTableUnsetColumnTag(
            PangrovaSqlParser.AlterTableUnsetColumnTagContext ctx) {
        return versionedTableDdl.visitAlterTableUnsetColumnTag(ctx);
    }

    @Override
    public AlterTableStatement visitAlterTableAdopt(
            PangrovaSqlParser.AlterTableAdoptContext ctx) {
        return versionedTableDdl.visitAlterTableAdopt(ctx);
    }

    @Override
    public AlterTableStatement visitAlterTableUnlink(
            PangrovaSqlParser.AlterTableUnlinkContext ctx) {
        return versionedTableDdl.visitAlterTableUnlink(ctx);
    }

    @Override
    public CreateIndexStatement visitCreateIndex(PangrovaSqlParser.CreateIndexContext ctx) {
        String indexName = AstDdlDmlBuilder.extractIdentifierText(ctx.indexName);
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        List<String> columns = ctx.columnNameList().identifier().stream()
                .map(AstDdlDmlBuilder::extractIdentifierText).toList();
        String indexType = AstDdlDmlBuilder.extractIdentifierText(ctx.indexTypeName)
                .toUpperCase(java.util.Locale.ROOT);
        boolean ifNotExists = ctx.EXISTS() != null;
        Map<String, String> properties = Map.of();
        if (ctx.stringPropertyList() != null) {
            Map<String, String> props = new LinkedHashMap<>();
            for (PangrovaSqlParser.StringPropertyContext sp : ctx.stringPropertyList().stringProperty()) {
                props.put(AstDdlDmlBuilder.stripQuotes(sp.key.getText()),
                        AstDdlDmlBuilder.stripQuotes(sp.value.getText()));
            }
            properties = props;
        }
        boolean force = ctx.FORCE() != null;
        return new CreateIndexStatement(indexName, tableName, columns, indexType,
                ifNotExists, properties, force);
    }

    @Override
    public DropIndexStatement visitDropIndex(PangrovaSqlParser.DropIndexContext ctx) {
        String indexName = AstDdlDmlBuilder.extractIdentifierText(ctx.indexName);
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        boolean ifExists = ctx.EXISTS() != null;
        return new DropIndexStatement(indexName, tableName, ifExists);
    }

    @Override
    public ShowIndexesStatement visitShowIndexes(PangrovaSqlParser.ShowIndexesContext ctx) {
        return new ShowIndexesStatement(parent.visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public AlterIndexStatement visitAlterIndex(PangrovaSqlParser.AlterIndexContext ctx) {
        String indexName = AstDdlDmlBuilder.extractIdentifierText(ctx.indexName);
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        return new AlterIndexStatement(indexName, tableName);
    }

    @Override
    public CreateBranchStatement visitCreateBranch(PangrovaSqlParser.CreateBranchContext ctx) {
        return versionedTableDdl.visitCreateBranch(ctx);
    }

    @Override
    public DropBranchStatement visitDropBranch(PangrovaSqlParser.DropBranchContext ctx) {
        return versionedTableDdl.visitDropBranch(ctx);
    }

    @Override
    public CreateTagStatement visitCreateTag(PangrovaSqlParser.CreateTagContext ctx) {
        return versionedTableDdl.visitCreateTag(ctx);
    }

    @Override
    public DropTagStatement visitDropTag(PangrovaSqlParser.DropTagContext ctx) {
        return versionedTableDdl.visitDropTag(ctx);
    }

    @Override
    public UseBranchStatement visitUseBranch(PangrovaSqlParser.UseBranchContext ctx) {
        return versionedTableDdl.visitUseBranch(ctx);
    }

    @Override
    public ShowBranchesStatement visitShowBranches(PangrovaSqlParser.ShowBranchesContext ctx) {
        return versionedTableDdl.visitShowBranches(ctx);
    }

    @Override
    public ShowTagsStatement visitShowTags(PangrovaSqlParser.ShowTagsContext ctx) {
        return versionedTableDdl.visitShowTags(ctx);
    }

    @Override
    public SetClusteringOrderStatement visitSetClusteringOrder(
            PangrovaSqlParser.SetClusteringOrderContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        List<String> columns = ctx.columnNameList().identifier().stream()
                .map(AstDdlDmlBuilder::extractIdentifierText).toList();
        String mode = ctx.clusteringMode != null
                ? ctx.clusteringMode.getText().toUpperCase(java.util.Locale.ROOT) : null;
        return new SetClusteringOrderStatement(tableName, columns, mode);
    }

    @Override
    public ReclusterTableStatement visitReclusterTable(
            PangrovaSqlParser.ReclusterTableContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        Optional<Expression> where = ctx.where != null
                ? Optional.of((Expression) parent.visit(ctx.where))
                : Optional.empty();
        return new ReclusterTableStatement(tableName, where);
    }

    @Override
    public ShowClusteringStatusStatement visitShowClusteringStatus(
            PangrovaSqlParser.ShowClusteringStatusContext ctx) {
        return new ShowClusteringStatusStatement(parent.visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public CreateStreamStatement visitCreateStream(PangrovaSqlParser.CreateStreamContext ctx) {
        Identifier name = Identifier.of(AstDdlDmlBuilder.extractIdentifierText(ctx.name));
        boolean ifNotExists = ctx.EXISTS() != null;
        Map<String, String> sourceProperties = visitStreamPropertyMap(ctx.streamPropertyList(0));
        String sourceFormat = AstDdlDmlBuilder.stripQuotes(ctx.sourceFormat.getText());
        Identifier targetTable = parent.visitQualifiedName(ctx.targetTable);
        Map<String, String> withProperties = ctx.streamPropertyList().size() > 1
                ? visitStreamPropertyMap(ctx.streamPropertyList(1))
                : Map.of();
        return new CreateStreamStatement(name, ifNotExists, sourceProperties,
                sourceFormat, targetTable, withProperties);
    }
    @Override
    public AlterStreamStatement visitAlterStreamSuspend(
            PangrovaSqlParser.AlterStreamSuspendContext ctx) {
        return new AlterStreamStatement(
                Identifier.of(AstDdlDmlBuilder.extractIdentifierText(ctx.name)),
                new AlterStreamAction.Suspend());
    }
    @Override
    public AlterStreamStatement visitAlterStreamResume(
            PangrovaSqlParser.AlterStreamResumeContext ctx) {
        return new AlterStreamStatement(
                Identifier.of(AstDdlDmlBuilder.extractIdentifierText(ctx.name)),
                new AlterStreamAction.Resume());
    }
    @Override
    public AlterStreamStatement visitAlterStreamSetProperties(
            PangrovaSqlParser.AlterStreamSetPropertiesContext ctx) {
        Map<String, String> properties = visitStreamPropertyMap(ctx.streamPropertyList());
        return new AlterStreamStatement(
                Identifier.of(AstDdlDmlBuilder.extractIdentifierText(ctx.name)),
                new AlterStreamAction.SetProperties(properties));
    }
    @Override
    public DropStreamStatement visitDropStream(PangrovaSqlParser.DropStreamContext ctx) {
        Identifier name = Identifier.of(AstDdlDmlBuilder.extractIdentifierText(ctx.name));
        boolean ifExists = ctx.EXISTS() != null;
        return new DropStreamStatement(name, ifExists);
    }
    @Override
    public ShowStreamsStatement visitShowStreams(PangrovaSqlParser.ShowStreamsContext ctx) {
        return new ShowStreamsStatement();
    }
    @Override
    public DescribeStreamStatement visitDescribeStream(PangrovaSqlParser.DescribeStreamContext ctx) {
        return new DescribeStreamStatement(
                Identifier.of(AstDdlDmlBuilder.extractIdentifierText(ctx.name)));
    }
    @Override
    public CreateEmbeddingModelStatement visitCreateEmbeddingModel(
            PangrovaSqlParser.CreateEmbeddingModelContext ctx) {
        String modelName = AstDdlDmlBuilder.extractIdentifierText(ctx.modelName);
        Map<String, String> properties = visitEmbeddingPropertyMap(ctx.embeddingPropertyList());
        return new CreateEmbeddingModelStatement(modelName, properties);
    }
    @Override
    public AlterEmbeddingModelStatement visitAlterEmbeddingModel(
            PangrovaSqlParser.AlterEmbeddingModelContext ctx) {
        String modelName = AstDdlDmlBuilder.extractIdentifierText(ctx.modelName);
        Map<String, String> properties = visitEmbeddingPropertyMap(ctx.embeddingPropertyList());
        return new AlterEmbeddingModelStatement(modelName, properties);
    }
    @Override
    public DropEmbeddingModelStatement visitDropEmbeddingModel(
            PangrovaSqlParser.DropEmbeddingModelContext ctx) {
        String modelName = AstDdlDmlBuilder.extractIdentifierText(ctx.modelName);
        boolean cascade = ctx.CASCADE() != null;
        return new DropEmbeddingModelStatement(modelName, cascade);
    }
    @Override
    public ShowEmbeddingModelsStatement visitShowEmbeddingModels(
            PangrovaSqlParser.ShowEmbeddingModelsContext ctx) {
        return new ShowEmbeddingModelsStatement();
    }
    @Override
    public BackfillColumnStatement visitBackfillColumn(PangrovaSqlParser.BackfillColumnContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.tableName);
        String columnName = AstDdlDmlBuilder.extractIdentifierText(ctx.columnName);
        Optional<Expression> where = ctx.where != null
                ? Optional.of((Expression) parent.visit(ctx.where))
                : Optional.empty();
        OptionalInt batchSize = parseOptionalInt(ctx.batchSize, "batch size");
        OptionalInt concurrency = parseOptionalInt(ctx.concurrency, "concurrency value");
        return new BackfillColumnStatement(tableName, columnName, where, batchSize, concurrency);
    }
    @Override
    public CancelBackfillStatement visitCancelBackfill(PangrovaSqlParser.CancelBackfillContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.tableName);
        String columnName = AstDdlDmlBuilder.extractIdentifierText(ctx.columnName);
        return new CancelBackfillStatement(tableName, columnName);
    }

    @Override
    public ShowBackfillStatusStatement visitShowBackfillStatus(
            PangrovaSqlParser.ShowBackfillStatusContext ctx) {
        return new ShowBackfillStatusStatement(parent.visitQualifiedName(ctx.tableName));
    }

    @Override
    public CreateStorageProfileStatement visitCreateStorageProfile(
            PangrovaSqlParser.CreateStorageProfileContext ctx) {
        String profileName = AstDdlDmlBuilder.extractIdentifierText(ctx.profileName);
        boolean ifNotExists = ctx.EXISTS() != null;
        Map<String, String> properties = visitStorageProfileProperties(ctx.storageProfilePropertyList());
        return new CreateStorageProfileStatement(profileName, ifNotExists, properties);
    }

    @Override
    public DropStorageProfileStatement visitDropStorageProfile(
            PangrovaSqlParser.DropStorageProfileContext ctx) {
        String profileName = AstDdlDmlBuilder.extractIdentifierText(ctx.profileName);
        boolean ifExists = ctx.EXISTS() != null;
        return new DropStorageProfileStatement(profileName, ifExists);
    }

    @Override
    public ShowStorageProfilesStatement visitShowStorageProfiles(
            PangrovaSqlParser.ShowStorageProfilesContext ctx) {
        return new ShowStorageProfilesStatement();
    }

    private Map<String, String> visitStorageProfileProperties(
            PangrovaSqlParser.StorageProfilePropertyListContext ctx) {
        Map<String, String> props = new LinkedHashMap<>();
        for (PangrovaSqlParser.StorageProfilePropertyContext prop : ctx.storageProfileProperty()) {
            String key;
            if (prop.skey != null) {
                key = AstDdlDmlBuilder.stripQuotes(prop.skey.getText());
            } else {
                key = AstDdlDmlBuilder.extractIdentifierText(prop.key);
            }
            String value = prop.value != null
                    ? AstDdlDmlBuilder.stripQuotes(prop.value.getText())
                    : "";
            props.put(key, value);
        }
        return Map.copyOf(props);
    }

    private Map<String, String> visitDtProperties(PangrovaSqlParser.StringPropertyListContext ctx) {
        Map<String, String> props = new LinkedHashMap<>();
        for (PangrovaSqlParser.StringPropertyContext sp : ctx.stringProperty()) {
            props.put(AstDdlDmlBuilder.stripQuotes(sp.key.getText()),
                    AstDdlDmlBuilder.stripQuotes(sp.value.getText()));
        }
        return Map.copyOf(props);
    }

    private Duration parsePositiveTargetLag(org.antlr.v4.runtime.tree.TerminalNode node) {
        Duration parsed = parseIso8601Duration(node);
        if (parsed.isZero() || parsed.isNegative()) {
            throw new ParseException(
                    org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    "TARGET_LAG must be a positive duration; omit TARGET_LAG for manual refresh",
                    node.getSymbol().getLine(), node.getSymbol().getCharPositionInLine());
        }
        return parsed;
    }

    private Duration parseIso8601Duration(org.antlr.v4.runtime.tree.TerminalNode node) {
        String raw = AstDdlDmlBuilder.stripQuotes(node.getText());
        try {
            return Duration.parse(raw);
        } catch (DateTimeParseException e) {
            int line = node.getSymbol().getLine();
            int col = node.getSymbol().getCharPositionInLine();
            throw new ParseException(
                    org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    "Invalid TARGET_LAG duration '" + raw
                            + "'. Expected ISO-8601 format (e.g., 'PT5M', 'PT1H', 'PT30S')",
                    line, col);
        }
    }

    private RefreshMode visitDtRefreshMode(PangrovaSqlParser.DtRefreshModeContext ctx) {
        if (ctx instanceof PangrovaSqlParser.SnapshotRefreshModeContext) return RefreshMode.SNAPSHOT;
        if (ctx instanceof PangrovaSqlParser.FullRefreshModeContext) return RefreshMode.FULL;
        if (ctx instanceof PangrovaSqlParser.CdcRefreshModeContext) return RefreshMode.CDC;
        if (ctx instanceof PangrovaSqlParser.AutoRefreshModeContext) return RefreshMode.AUTO;
        if (ctx instanceof PangrovaSqlParser.IncrementalRefreshModeContext) {
            return RefreshMode.INCREMENTAL;
        }
        throw new IllegalArgumentException("Unknown refresh mode: " + ctx.getText());
    }

    private static Optional<org.pangrova.parser.ast.IncrementalStrategy> incrementalStrategy(
            PangrovaSqlParser.IncrementalStrategyContext ctx) {
        if (ctx == null) {
            return Optional.empty();
        }
        return Optional.of(strategyOf(ctx.getText()));
    }

    private static org.pangrova.parser.ast.IncrementalStrategy strategyOf(String text) {
        return switch (text.toUpperCase(java.util.Locale.ROOT)) {
            case "AUTO" -> org.pangrova.parser.ast.IncrementalStrategy.AUTO;
            case "APPEND_ONLY" -> org.pangrova.parser.ast.IncrementalStrategy.APPEND_ONLY;
            case "PARTITION_REPLACE" ->
                    org.pangrova.parser.ast.IncrementalStrategy.PARTITION_REPLACE;
            case "AFFECTED_KEY" -> org.pangrova.parser.ast.IncrementalStrategy.AFFECTED_KEY;
            case "CDC" -> org.pangrova.parser.ast.IncrementalStrategy.CDC;
            default -> throw new IllegalArgumentException("Unknown incremental strategy: " + text);
        };
    }

    private Map<String, String> visitStreamPropertyMap(
            PangrovaSqlParser.StreamPropertyListContext ctx) {
        Map<String, String> props = new LinkedHashMap<>();
        for (PangrovaSqlParser.StreamPropertyContext sp : ctx.streamProperty()) {
            if (sp instanceof PangrovaSqlParser.StreamStringPropertyContext ssp) {
                props.put(AstDdlDmlBuilder.stripQuotes(ssp.stringKey.getText()),
                        AstDdlDmlBuilder.stripQuotes(ssp.stringValue.getText()));
            } else if (sp instanceof PangrovaSqlParser.StreamIdentPropertyContext sip) {
                String key = AstDdlDmlBuilder.extractIdentifierText(sip.identKey);
                String value = sip.identValue.getType() == PangrovaSqlParser.STRING_LITERAL
                        ? AstDdlDmlBuilder.stripQuotes(sip.identValue.getText())
                        : sip.identValue.getText();
                props.put(key, value);
            }
        }
        return props;
    }

    private Map<String, String> visitEmbeddingPropertyMap(
            PangrovaSqlParser.EmbeddingPropertyListContext ctx) {
        Map<String, String> props = new LinkedHashMap<>();
        for (PangrovaSqlParser.EmbeddingPropertyContext prop : ctx.embeddingProperty()) {
            String key = AstDdlDmlBuilder.extractIdentifierText(prop.key);
            String value = prop.value.getType() == PangrovaSqlParser.STRING_LITERAL
                    ? AstDdlDmlBuilder.stripQuotes(prop.value.getText())
                    : prop.value.getText();
            if (props.putIfAbsent(key, value) != null) {
                throw new IllegalArgumentException(
                        "Duplicate embedding model property: " + key);
            }
        }
        return props;
    }

    private OptionalInt parseOptionalInt(org.antlr.v4.runtime.Token token, String label) {
        if (token == null) {
            return OptionalInt.empty();
        }
        String text = token.getText();
        try {
            return OptionalInt.of(Integer.parseInt(text));
        } catch (NumberFormatException e) {
            var loc = new SourceLocation(token.getLine(), token.getCharPositionInLine(),
                    token.getTokenIndex(), token.getTokenIndex());
            throw new ParseException(
                    org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    "Invalid " + label + ": '" + text + "'", loc, e);
        }
    }
}
