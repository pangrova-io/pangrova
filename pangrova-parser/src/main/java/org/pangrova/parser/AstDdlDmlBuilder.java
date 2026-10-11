package org.pangrova.parser;

import org.pangrova.parser.ast.*;
import org.pangrova.parser.ast.statement.*;
import org.pangrova.parser.ast.statement.maintenance.CallStatement;
import org.pangrova.parser.ast.statement.maintenance.CompactStatement;
import org.pangrova.parser.ast.statement.maintenance.OptimizeStatement;
import org.pangrova.parser.ast.statement.maintenance.VacuumStatement;
import org.pangrova.parser.ast.type.BaseType;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.parser.ast.type.DistanceMetric;
import org.pangrova.parser.ast.type.FieldSpec;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Handles DDL and DML parse-tree visitor methods, delegated from AstBuilder.
 * Covers: CREATE/DROP/ALTER TABLE, INSERT, UPDATE, DELETE, MERGE,
 * SHOW/DESCRIBE, Catalog DDL, Dynamic Table DDL, and maintenance statements.
 */
class AstDdlDmlBuilder extends PangrovaSqlBaseVisitor<Object> {

    private static final int DEFAULT_DECIMAL_PRECISION = 38;

    private final AstBuilder parent;

    AstDdlDmlBuilder(AstBuilder parent) {
        this.parent = parent;
    }

    @Override
    public CompactStatement visitCompactTable(PangrovaSqlParser.CompactTableContext ctx) {
        return new CompactStatement(parent.visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public OptimizeStatement visitOptimizeTable(PangrovaSqlParser.OptimizeTableContext ctx) {
        return new OptimizeStatement(parent.visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public VacuumStatement visitVacuumTable(PangrovaSqlParser.VacuumTableContext ctx) {
        return new VacuumStatement(parent.visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public CreateCatalogStatement visitCreateCatalog(PangrovaSqlParser.CreateCatalogContext ctx) {
        String name = extractIdentifierText(ctx.name);
        Optional<String> connectorType = ctx.connectorType != null
                ? Optional.of(extractIdentifierText(ctx.connectorType))
                : Optional.empty();
        Map<String, Literal> props = visitPropertyMap(ctx.propertyList());
        return new CreateCatalogStatement(name, connectorType, props);
    }

    @Override
    public DropCatalogStatement visitDropCatalog(PangrovaSqlParser.DropCatalogContext ctx) {
        return new DropCatalogStatement(extractIdentifierText(ctx.identifier()));
    }

    @Override
    public ShowCatalogsStatement visitShowCatalogs(PangrovaSqlParser.ShowCatalogsContext ctx) {
        return new ShowCatalogsStatement();
    }

    @Override
    public DescribeCatalogStatement visitDescribeCatalog(PangrovaSqlParser.DescribeCatalogContext ctx) {
        return new DescribeCatalogStatement(extractIdentifierText(ctx.identifier()));
    }

    @Override
    public AlterCatalogStatement visitAlterCatalog(PangrovaSqlParser.AlterCatalogContext ctx) {
        String name = extractIdentifierText(ctx.identifier());
        Map<String, Literal> props = visitPropertyMap(ctx.propertyList());
        return new AlterCatalogStatement(name, props);
    }

    @Override
    public CallStatement visitCallStatement(PangrovaSqlParser.CallStatementContext ctx) {
        Identifier name = parent.visitQualifiedName(ctx.qualifiedName());
        List<Expression> args = ctx.expression().stream()
                .map(e -> (Expression) parent.visit(e))
                .toList();
        return new CallStatement(name, args);
    }
    @Override
    public CreateTableStatement visitCreateTableColumns(PangrovaSqlParser.CreateTableColumnsContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        boolean ifNotExists = ctx.IF() != null;

        List<CreateTableStatement.ColumnDefinition> columns = new java.util.ArrayList<>();
        List<String> primaryKeys = List.of();
        for (PangrovaSqlParser.TableElementContext elem : ctx.tableElementList().tableElement()) {
            if (elem instanceof PangrovaSqlParser.ColumnDefContext cd) {
                columns.add(buildColumnDef(cd));
            } else if (elem instanceof PangrovaSqlParser.PrimaryKeyDefContext pk) {
                primaryKeys = pk.columnNameList().identifier().stream()
                        .map(AstDdlDmlBuilder::extractIdentifierText).toList();
            }
        }

        List<String> partitionCols = visitPartitionColumns(ctx.partitionColumnList());

        List<AlterTableStatement.SortColumn> sortCols = List.of();
        boolean useZOrder = false;
        if (ctx.sortColumnList() != null) {
            sortCols = ctx.sortColumnList().sortColumn().stream()
                    .map(sc -> {
                        String colName = extractIdentifierText(sc.identifier());
                        boolean asc = sc.DESC() == null;
                        return new AlterTableStatement.SortColumn(colName, asc);
                    })
                    .toList();
            useZOrder = ctx.ZORDER() != null;
        }

        Optional<String> comment = extractComment(ctx.STRING_LITERAL());
        Map<String, String> props = visitTableProps(ctx.tableProperties());
        return new CreateTableStatement(tableName, List.copyOf(columns),
                ifNotExists, primaryKeys, partitionCols, sortCols, useZOrder, comment, props);
    }

    @Override
    public org.pangrova.parser.ast.statement.CreateTableAsSelectStatement visitCreateTableAsSelect(
            PangrovaSqlParser.CreateTableAsSelectContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        boolean ifNotExists = ctx.IF() != null;
        Statement query = (Statement) parent.visit(ctx.setQuery());
        return new org.pangrova.parser.ast.statement.CreateTableAsSelectStatement(tableName, ifNotExists, query);
    }

    @Override
    public DropTableStatement visitDropTable(PangrovaSqlParser.DropTableContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        boolean ifExists = ctx.IF() != null;
        return new DropTableStatement(tableName, ifExists);
    }

    @Override
    public ShowSchemasStatement visitShowSchemas(PangrovaSqlParser.ShowSchemasContext ctx) {
        String catalogName = ctx.identifier() != null
                ? extractIdentifierText(ctx.identifier())
                : "default";
        return new ShowSchemasStatement(catalogName);
    }

    @Override
    public ShowTablesStatement visitShowTables(PangrovaSqlParser.ShowTablesContext ctx) {
        if (ctx.qualifiedName() != null) {
            List<String> parts = ctx.qualifiedName().identifier().stream()
                    .map(AstDdlDmlBuilder::extractIdentifierText).toList();
            return new ShowTablesStatement(parts);
        }
        return new ShowTablesStatement(List.of("default"));
    }

    @Override
    public DescribeTableStatement visitDescribeTable(PangrovaSqlParser.DescribeTableContext ctx) {
        return new DescribeTableStatement(parent.visitQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public CreateSchemaStatement visitCreateSchema(PangrovaSqlParser.CreateSchemaContext ctx) {
        Identifier schemaName = parent.visitQualifiedName(ctx.qualifiedName());
        boolean ifNotExists = ctx.IF() != null;
        return new CreateSchemaStatement(schemaName, ifNotExists);
    }

    @Override
    public AlterTableStatement visitAlterTableAddColumn(
            PangrovaSqlParser.AlterTableAddColumnContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        PangrovaSqlParser.ColumnDefinitionContext cd = ctx.columnDefinition();
        String colName = extractIdentifierText(cd.identifier());
        DataTypeSpec colType = buildDataTypeSpec(cd.columnType());
        Optional<Literal> defaultValue = ctx.defaultVal != null
                ? Optional.of((Literal) parent.visit(ctx.defaultVal))
                : Optional.empty();
        Optional<AutoEmbeddingColumnSpec> embeddingSpec = buildAutoEmbeddingSpec(
                cd.autoEmbeddingClause());
        return new AlterTableStatement.AddColumn(tableName, colName, colType,
                defaultValue, embeddingSpec);
    }

    @Override
    public AlterTableStatement visitAlterTableDropColumn(
            PangrovaSqlParser.AlterTableDropColumnContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        String colName = extractIdentifierText(ctx.identifier());
        return new AlterTableStatement.DropColumn(tableName, colName);
    }

    @Override
    public AlterTableStatement visitAlterTableRenameColumn(
            PangrovaSqlParser.AlterTableRenameColumnContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        String oldName = extractIdentifierText(ctx.old);
        String newName = extractIdentifierText(ctx.new_);
        return new AlterTableStatement.RenameColumn(tableName, oldName, newName);
    }

    @Override
    public AlterTableStatement visitAlterTableChangeType(
            PangrovaSqlParser.AlterTableChangeTypeContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        String colName = extractIdentifierText(ctx.identifier());
        DataTypeSpec newType = buildDataTypeSpec(ctx.columnType());
        return new AlterTableStatement.ChangeColumnType(tableName, colName, newType);
    }

    @Override
    public AlterTableStatement visitAlterTableSetPartitionSpec(
            PangrovaSqlParser.AlterTableSetPartitionSpecContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        List<AlterTableStatement.PartitionTransform> transforms =
                ctx.partitionTransformList().partitionTransform().stream()
                        .map(this::visitPartitionTransformNode)
                        .toList();
        return new AlterTableStatement.SetPartitionSpec(tableName, transforms);
    }

    @Override
    public AlterTableStatement visitAlterTableSetSortOrder(
            PangrovaSqlParser.AlterTableSetSortOrderContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        List<AlterTableStatement.SortColumn> sortCols =
                ctx.sortColumnList().sortColumn().stream()
                        .map(sc -> {
                            String colName = extractIdentifierText(sc.identifier());
                            boolean asc = sc.DESC() == null;
                            return new AlterTableStatement.SortColumn(colName, asc);
                        })
                        .toList();
        boolean useZOrder = ctx.ZORDER() != null;
        return new AlterTableStatement.SetSortOrder(tableName, sortCols, useZOrder);
    }

    @Override
    public AlterTableStatement visitAlterTableRename(
            PangrovaSqlParser.AlterTableRenameContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName(0));
        Identifier newName = parent.visitQualifiedName(ctx.newName);
        return new AlterTableStatement.RenameTable(tableName, newName);
    }

    @Override
    public CreateViewStatement visitCreateView(PangrovaSqlParser.CreateViewContext ctx) {
        Identifier viewName = parent.visitQualifiedName(ctx.qualifiedName());
        boolean ifNotExists = ctx.EXISTS() != null;
        boolean orReplace = ctx.REPLACE() != null;
        var queryCtx = ctx.setQuery();
        String viewSql = parent.tokenStream().getTokenSource().getInputStream().getText(
                org.antlr.v4.runtime.misc.Interval.of(
                        queryCtx.start.getStartIndex(),
                        queryCtx.stop.getStopIndex()));
        return new CreateViewStatement(viewName, ifNotExists, orReplace, viewSql);
    }

    @Override
    public DropViewStatement visitDropView(PangrovaSqlParser.DropViewContext ctx) {
        Identifier viewName = parent.visitQualifiedName(ctx.qualifiedName());
        boolean ifExists = ctx.EXISTS() != null;
        return new DropViewStatement(viewName, ifExists);
    }

    @Override
    public ShowViewsStatement visitShowViews(PangrovaSqlParser.ShowViewsContext ctx) {
        if (ctx.qualifiedName() != null) {
            List<String> parts = ctx.qualifiedName().identifier().stream()
                    .map(AstDdlDmlBuilder::extractIdentifierText).toList();
            return new ShowViewsStatement(parts);
        }
        return new ShowViewsStatement(List.of("default"));
    }

    @Override
    public ShowMetadataStatement visitShowSnapshots(PangrovaSqlParser.ShowSnapshotsContext ctx) {
        List<String> parts = ctx.qualifiedName().identifier().stream()
                .map(AstDdlDmlBuilder::extractIdentifierText).toList();
        return new ShowMetadataStatement.ShowSnapshots(parts);
    }

    @Override
    public ShowMetadataStatement visitShowFiles(PangrovaSqlParser.ShowFilesContext ctx) {
        List<String> parts = ctx.qualifiedName().identifier().stream()
                .map(AstDdlDmlBuilder::extractIdentifierText).toList();
        return new ShowMetadataStatement.ShowFiles(parts);
    }

    @Override
    public ShowMetadataStatement visitShowManifests(PangrovaSqlParser.ShowManifestsContext ctx) {
        List<String> parts = ctx.qualifiedName().identifier().stream()
                .map(AstDdlDmlBuilder::extractIdentifierText).toList();
        return new ShowMetadataStatement.ShowManifests(parts);
    }

    @Override
    public ShowMetadataStatement visitShowPartitions(PangrovaSqlParser.ShowPartitionsContext ctx) {
        List<String> parts = ctx.qualifiedName().identifier().stream()
                .map(AstDdlDmlBuilder::extractIdentifierText).toList();
        return new ShowMetadataStatement.ShowPartitions(parts);
    }

    private AlterTableStatement.PartitionTransform visitPartitionTransformNode(
            PangrovaSqlParser.PartitionTransformContext ctx) {
        if (ctx instanceof PangrovaSqlParser.FunctionTransformContext ft) {
            String funcName = extractIdentifierText(ft.identifier());
            List<String> args = ft.expressionList().expression().stream()
                    .map(e -> e.getText())
                    .toList();
            return AlterTableStatement.PartitionTransform.function(funcName, args);
        }
        if (ctx instanceof PangrovaSqlParser.IdentityTransformContext it) {
            String colName = extractIdentifierText(it.identifier());
            return AlterTableStatement.PartitionTransform.identity(colName);
        }
        throw new IllegalArgumentException("Unknown partition transform: " + ctx.getText());
    }

    private CreateTableStatement.ColumnDefinition buildColumnDef(
            PangrovaSqlParser.ColumnDefContext ctx) {
        PangrovaSqlParser.ColumnDefinitionContext cd = ctx.columnDefinition();
        String name = extractIdentifierText(cd.identifier());
        DataTypeSpec type = buildDataTypeSpec(cd.columnType());
        boolean nullable = cd.NULL() == null; // NOT NULL present means nullable=false
        Optional<String> defaultValue = cd.defaultVal != null
                ? Optional.of(cd.defaultVal.getText())
                : Optional.empty();
        Optional<AutoEmbeddingColumnSpec> embeddingSpec = buildAutoEmbeddingSpec(
                cd.autoEmbeddingClause());
        return new CreateTableStatement.ColumnDefinition(name, type, nullable, defaultValue, embeddingSpec);
    }

    /**
     * Constructs a DataTypeSpec directly from grammar context tokens,
     * avoiding getText() concatenation which loses whitespace in nested types.
     */
    DataTypeSpec buildDataTypeSpec(PangrovaSqlParser.ColumnTypeContext ctx) {
        if (ctx.ARRAY() != null) {
            return new DataTypeSpec.ArrayType(buildDataTypeSpec(ctx.elementType));
        }
        if (ctx.MAP() != null) {
            return new DataTypeSpec.MapType(
                    buildDataTypeSpec(ctx.keyType), buildDataTypeSpec(ctx.valueType));
        }
        if (ctx.STRUCT() != null) {
            List<FieldSpec> fields = ctx.structField().stream()
                    .map(sf -> new FieldSpec(
                            extractIdentifierText(sf.fieldName),
                            buildDataTypeSpec(sf.fieldType)))
                    .toList();
            return new DataTypeSpec.StructType(fields);
        }
        if (ctx.VECTOR() != null) {
            return buildVectorType(ctx);
        }
        if (ctx.FILE_TYPE_KW() != null) {
            return DataTypeSpec.FileType.INSTANCE;
        }
        if (ctx.DECIMAL() != null || ctx.NUMERIC() != null) {
            return buildDecimalType(ctx);
        }
        return new DataTypeSpec.PrimitiveType(resolvePrimitiveBaseType(ctx));
    }

    private DataTypeSpec.VectorType buildVectorType(PangrovaSqlParser.ColumnTypeContext ctx) {
        int dimension;
        try {
            dimension = Integer.parseInt(ctx.vectorDim.getText());
        } catch (NumberFormatException e) {
            var loc = new SourceLocation(ctx.vectorDim.getLine(), ctx.vectorDim.getCharPositionInLine(),
                    ctx.vectorDim.getTokenIndex(), ctx.vectorDim.getTokenIndex());
            throw new ParseException(
                    org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                    "Invalid vector dimension: '" + ctx.vectorDim.getText() + "'", loc, e);
        }
        var element = ctx.vectorElementType != null && ctx.vectorElementType.getType() == PangrovaSqlParser.DOUBLE
                ? org.pangrova.spi.connector.data.type.VectorType.ElementType.DOUBLE
                : org.pangrova.spi.connector.data.type.VectorType.ElementType.FLOAT;
        return new DataTypeSpec.VectorType(dimension, DistanceMetric.COSINE, element);
    }

    private DataTypeSpec.DecimalType buildDecimalType(PangrovaSqlParser.ColumnTypeContext ctx) {
        int precision;
        if (ctx.precision != null) {
            try {
                precision = Integer.parseInt(ctx.precision.getText());
            } catch (NumberFormatException e) {
                var loc = new SourceLocation(ctx.precision.getLine(), ctx.precision.getCharPositionInLine(),
                        ctx.precision.getTokenIndex(), ctx.precision.getTokenIndex());
                throw new ParseException(
                        org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                        "Invalid decimal precision: '" + ctx.precision.getText() + "'", loc, e);
            }
        } else {
            precision = DEFAULT_DECIMAL_PRECISION;
        }
        int scale;
        if (ctx.scale != null) {
            try {
                scale = Integer.parseInt(ctx.scale.getText());
            } catch (NumberFormatException e) {
                var loc = new SourceLocation(ctx.scale.getLine(), ctx.scale.getCharPositionInLine(),
                        ctx.scale.getTokenIndex(), ctx.scale.getTokenIndex());
                throw new ParseException(
                        org.pangrova.common.exception.PangrovaErrorCode.PARSER_SYNTAX_ERROR,
                        "Invalid decimal scale: '" + ctx.scale.getText() + "'", loc, e);
            }
        } else {
            scale = 0;
        }
        return new DataTypeSpec.DecimalType(precision, scale);
    }

    private BaseType resolvePrimitiveBaseType(PangrovaSqlParser.ColumnTypeContext ctx) {
        if (ctx.VARCHAR() != null) return BaseType.VARCHAR;
        if (ctx.VARBINARY() != null) return BaseType.VARBINARY;
        if (ctx.BIGINT() != null) return BaseType.BIGINT;
        if (ctx.INT() != null || ctx.INTEGER() != null) return BaseType.INTEGER;
        if (ctx.DOUBLE() != null) return BaseType.DOUBLE;
        if (ctx.FLOAT() != null) return BaseType.FLOAT;
        if (ctx.BOOLEAN() != null) return BaseType.BOOLEAN;
        if (ctx.DATE() != null) return BaseType.DATE;
        if (ctx.TIMESTAMP() != null) return BaseType.TIMESTAMP;
        if (ctx.TIMESTAMP_NS() != null) return BaseType.TIMESTAMP_NS;
        if (ctx.STRING_TYPE() != null) return BaseType.STRING;
        if (ctx.TINYINT() != null) return BaseType.TINYINT;
        if (ctx.SMALLINT() != null) return BaseType.SMALLINT;
        if (ctx.VARIANT() != null) return BaseType.VARIANT;
        if (ctx.GEOMETRY() != null) return BaseType.GEOMETRY;
        if (ctx.GEOGRAPHY() != null) return BaseType.GEOGRAPHY;
        if (ctx.BLOB() != null) return BaseType.BLOB;
        throw new IllegalArgumentException("Unrecognized column type: " + ctx.getText());
    }

    private Optional<AutoEmbeddingColumnSpec> buildAutoEmbeddingSpec(
            PangrovaSqlParser.AutoEmbeddingClauseContext ctx) {
        if (ctx == null) {
            return Optional.empty();
        }
        String modelName = stripQuotes(ctx.modelName.getText());
        List<String> sourceColumns = ctx.sourceColumn.stream()
                .map(AstDdlDmlBuilder::extractIdentifierText)
                .toList();
        AutoEmbeddingColumnSpec.EmbeddingMode mode = AutoEmbeddingColumnSpec.EmbeddingMode.SYNC;
        if (ctx.embeddingMode != null && ctx.embeddingMode.getType() == PangrovaSqlParser.ASYNC) {
            mode = AutoEmbeddingColumnSpec.EmbeddingMode.ASYNC;
        }
        return Optional.of(new AutoEmbeddingColumnSpec(modelName, sourceColumns, mode));
    }

    private Map<String, String> visitTableProps(PangrovaSqlParser.TablePropertiesContext ctx) {
        if (ctx == null) return Map.of();
        Map<String, String> props = new LinkedHashMap<>();
        for (PangrovaSqlParser.StringPropertyContext sp : ctx.stringPropertyList().stringProperty()) {
            props.put(stripQuotes(sp.key.getText()), stripQuotes(sp.value.getText()));
        }
        return props;
    }

    List<String> visitPartitionColumns(PangrovaSqlParser.PartitionColumnListContext ctx) {
        if (ctx == null) {
            return List.of();
        }
        return ctx.identifier().stream().map(AstDdlDmlBuilder::extractIdentifierText).toList();
    }

    Optional<String> extractComment(org.antlr.v4.runtime.tree.TerminalNode node) {
        if (node == null) {
            return Optional.empty();
        }
        String raw = node.getText();
        return Optional.of(raw.substring(1, raw.length() - 1).replace("''", "'"));
    }

    private Map<String, Literal> visitPropertyMap(PangrovaSqlParser.PropertyListContext ctx) {
        Map<String, Literal> props = new LinkedHashMap<>();
        for (PangrovaSqlParser.PropertyContext prop : ctx.property()) {
            String key = extractPropertyKey(prop);
            Literal value = (Literal) parent.visit(prop.literal());
            props.put(key, value);
        }
        return props;
    }

    private String extractPropertyKey(PangrovaSqlParser.PropertyContext prop) {
        if (prop.key != null) {
            return extractIdentifierText(prop.key);
        }
        return stripQuotes(prop.stringKey.getText());
    }

    static String extractIdentifierText(PangrovaSqlParser.IdentifierContext ctx) {
        if (ctx.QUOTED_IDENTIFIER() != null) {
            String raw = ctx.QUOTED_IDENTIFIER().getText();
            return raw.substring(1, raw.length() - 1).replace("\"\"", "\"");
        }
        if (ctx.DOLLAR_IDENTIFIER() != null) {
            return ctx.DOLLAR_IDENTIFIER().getText();
        }
        return ctx.getText();
    }

    static String stripQuotes(String s) {
        if (s.length() >= 2 && s.startsWith("'") && s.endsWith("'")) {
            return s.substring(1, s.length() - 1).replace("''", "'");
        }
        return s;
    }
}
