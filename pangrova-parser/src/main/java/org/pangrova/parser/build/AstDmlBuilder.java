package org.pangrova.parser.build;

import org.pangrova.parser.build.antlr.PangrovaSqlBaseVisitor;
import org.pangrova.parser.build.antlr.PangrovaSqlParser;

import org.pangrova.parser.api.ast.*;
import org.pangrova.parser.api.ast.statement.*;
import org.pangrova.parser.api.ast.statement.dml.DeleteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertOverwriteStatement;
import org.pangrova.parser.api.ast.statement.dml.InsertSelectStatement;
import org.pangrova.parser.api.ast.statement.dml.MergeStatement;
import org.pangrova.parser.api.ast.statement.dml.UpdateStatement;
import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Handles DML parse-tree visitor methods: INSERT, INSERT OVERWRITE,
 * INSERT SELECT, DELETE, UPDATE, MERGE.
 * Delegated from {@link AstBuilder}.
 */
class AstDmlBuilder extends PangrovaSqlBaseVisitor<Object> {

    private final AstBuilder parent;
    private final AstDdlDmlBuilder ddlHelper;

    AstDmlBuilder(AstBuilder parent, AstDdlDmlBuilder ddlHelper) {
        this.parent = parent;
        this.ddlHelper = ddlHelper;
    }

    @Override
    public InsertStatement visitInsertInto(PangrovaSqlParser.InsertIntoContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        Optional<List<String>> columns = ctx.columnNameList() != null
                ? Optional.of(ctx.columnNameList().identifier().stream()
                        .map(AstDdlDmlBuilder::extractIdentifierText).toList())
                : Optional.empty();
        List<List<Expression>> values = ctx.valuesList().valueTuple().stream()
                .map(vt -> vt.expression().stream()
                        .map(e -> (Expression) parent.visit(e)).toList())
                .toList();
        return new InsertStatement(tableName, columns, values);
    }

    @Override
    public InsertOverwriteStatement visitInsertOverwrite(
            PangrovaSqlParser.InsertOverwriteContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        Optional<List<String>> columns = ctx.columnNameList() != null
                ? Optional.of(ctx.columnNameList().identifier().stream()
                        .map(AstDdlDmlBuilder::extractIdentifierText).toList())
                : Optional.empty();
        Optional<Map<String, Literal>> partitionSpec = ctx.partitionSpec() != null
                ? Optional.of(parsePartitionSpec(ctx.partitionSpec()))
                : Optional.empty();
        List<List<Expression>> values = ctx.valuesList().valueTuple().stream()
                .map(vt -> vt.expression().stream()
                        .map(e -> (Expression) parent.visit(e)).toList())
                .toList();
        return new InsertOverwriteStatement(tableName, columns, partitionSpec, values);
    }

    @Override
    public InsertSelectStatement visitInsertSelect(PangrovaSqlParser.InsertSelectContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        Optional<List<String>> columns = ctx.columnNameList() != null
                ? Optional.of(ctx.columnNameList().identifier().stream()
                        .map(AstDdlDmlBuilder::extractIdentifierText).toList())
                : Optional.empty();
        boolean overwrite = ctx.OVERWRITE() != null;
        Statement selectStmt = (Statement) parent.visit(ctx.setQuery());
        SetQuery selectQuery = (selectStmt instanceof SetQuery sq)
                ? sq : new SetQuery(List.of((Query) selectStmt));
        return new InsertSelectStatement(tableName, columns, overwrite, selectQuery);
    }

    @Override
    public InsertSelectStatement visitCopyInto(PangrovaSqlParser.CopyIntoContext ctx) {
        if (ctx.copySource().STRING_LITERAL() != null) {
            throw new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                    "COPY INTO from a URI is unsupported until file discovery, authorization, "
                            + "format scanning, and committed file-load tracking are implemented");
        }
        if (ctx.FILE_FORMAT() != null || ctx.PATTERN() != null || ctx.copyOptionList() != null) {
            throw new PangrovaException(PangrovaErrorCode.PARSER_UNSUPPORTED_FEATURE,
                    "COPY INTO FROM (SELECT ...) does not support file-loading options");
        }
        Statement source = (Statement) parent.visit(ctx.copySource().setQuery());
        SetQuery query = source instanceof SetQuery setQuery
                ? setQuery : new SetQuery(List.of((Query) source));
        return new InsertSelectStatement(parent.visitQualifiedName(ctx.qualifiedName()),
                Optional.empty(), false, query);
    }

    @Override
    public DeleteStatement visitDeleteFrom(PangrovaSqlParser.DeleteFromContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        Optional<Expression> where = ctx.expression() != null
                ? Optional.of((Expression) parent.visit(ctx.expression()))
                : Optional.empty();
        return new DeleteStatement(tableName, where);
    }

    @Override
    public UpdateStatement visitUpdateStatement(PangrovaSqlParser.UpdateStatementContext ctx) {
        Identifier tableName = parent.visitQualifiedName(ctx.qualifiedName());
        List<UpdateStatement.Assignment> assignments = visitSetClauseList(ctx.setClauseList());
        Optional<Expression> where = ctx.expression() != null
                ? Optional.of((Expression) parent.visit(ctx.expression()))
                : Optional.empty();
        return new UpdateStatement(tableName, assignments, where);
    }

    @Override
    public MergeStatement visitMergeInto(PangrovaSqlParser.MergeIntoContext ctx) {
        Identifier targetTable = parent.visitQualifiedName(ctx.qualifiedName());
        Optional<String> targetAlias = ctx.alias != null
                ? Optional.of(AstDdlDmlBuilder.extractIdentifierText(ctx.alias))
                : Optional.empty();
        TableRef sourceTable = (TableRef) parent.visit(ctx.tableRef());
        Expression onCondition = (Expression) parent.visit(ctx.expression());
        List<MergeStatement.MergeClause> clauses = ctx.mergeClause().stream()
                .map(mc -> (MergeStatement.MergeClause) parent.visit(mc))
                .toList();
        return new MergeStatement(targetTable, targetAlias, sourceTable, onCondition, clauses);
    }

    @Override
    public MergeStatement.MergeClause visitMergeWhenMatched(
            PangrovaSqlParser.MergeWhenMatchedContext ctx) {
        Optional<Expression> condition = ctx.expression() != null
                ? Optional.of((Expression) parent.visit(ctx.expression()))
                : Optional.empty();
        List<UpdateStatement.Assignment> assignments =
                visitSetClauseList(ctx.mergeUpdate().setClauseList());
        return new MergeStatement.MergeClause.WhenMatchedUpdate(condition, assignments);
    }

    @Override
    public MergeStatement.MergeClause visitMergeWhenMatchedDelete(
            PangrovaSqlParser.MergeWhenMatchedDeleteContext ctx) {
        Optional<Expression> condition = ctx.expression() != null
                ? Optional.of((Expression) parent.visit(ctx.expression()))
                : Optional.empty();
        return new MergeStatement.MergeClause.WhenMatchedDelete(condition);
    }

    @Override
    public MergeStatement.MergeClause visitMergeWhenNotMatched(
            PangrovaSqlParser.MergeWhenNotMatchedContext ctx) {
        Optional<Expression> condition = ctx.expression() != null
                ? Optional.of((Expression) parent.visit(ctx.expression()))
                : Optional.empty();
        PangrovaSqlParser.MergeInsertContext mi = ctx.mergeInsert();
        Optional<List<String>> columns = mi.columnNameList() != null
                ? Optional.of(mi.columnNameList().identifier().stream()
                        .map(AstDdlDmlBuilder::extractIdentifierText).toList())
                : Optional.empty();
        List<Expression> values = mi.valueTuple().expression().stream()
                .map(e -> (Expression) parent.visit(e)).toList();
        return new MergeStatement.MergeClause.WhenNotMatchedInsert(condition, columns, values);
    }

    public List<UpdateStatement.Assignment> visitSetClauseList(
            PangrovaSqlParser.SetClauseListContext ctx) {
        return ctx.setClause().stream()
                .map(sc -> new UpdateStatement.Assignment(
                        AstDdlDmlBuilder.extractIdentifierText(sc.identifier()),
                        (Expression) parent.visit(sc.expression())))
                .toList();
    }

    private Map<String, Literal> parsePartitionSpec(PangrovaSqlParser.PartitionSpecContext ctx) {
        Map<String, Literal> spec = new LinkedHashMap<>();
        for (PangrovaSqlParser.PartitionAssignmentContext pa : ctx.partitionAssignment()) {
            String key = AstDdlDmlBuilder.extractIdentifierText(pa.identifier());
            Literal value = (Literal) parent.visit(pa.literal());
            spec.put(key, value);
        }
        return spec;
    }
}
