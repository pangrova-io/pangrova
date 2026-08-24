package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.ImmutableExpression;
import io.substrait.relation.ImmutableFilter;
import io.substrait.relation.ImmutableFetch;
import io.substrait.relation.ImmutableJoin;
import io.substrait.relation.ImmutableProject;
import io.substrait.relation.ImmutableSort;
import io.substrait.relation.ImmutableVirtualTableScan;
import io.substrait.relation.Join;
import io.substrait.relation.Rel;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.pangrova.common.exception.PangrovaErrorCode;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Lightweight proto-to-Rel deserializer for Phase 0 scope.
 * Avoids ProtoPlanConverter which triggers ANTLR 4.9.2/4.13.1 version conflict.
 * Supports: ProjectRel, ReadRel(VirtualTable), FilterRel, JoinRel, SortRel, FetchRel, Literals.
 */
final class SubstraitPlanDeserializer {

    private static final Map<io.substrait.proto.JoinRel.JoinType, Join.JoinType> JOIN_TYPE_MAP = Map.of(
            io.substrait.proto.JoinRel.JoinType.JOIN_TYPE_INNER, Join.JoinType.INNER,
            io.substrait.proto.JoinRel.JoinType.JOIN_TYPE_LEFT, Join.JoinType.LEFT,
            io.substrait.proto.JoinRel.JoinType.JOIN_TYPE_RIGHT, Join.JoinType.RIGHT,
            io.substrait.proto.JoinRel.JoinType.JOIN_TYPE_OUTER, Join.JoinType.OUTER,
            io.substrait.proto.JoinRel.JoinType.JOIN_TYPE_SEMI, Join.JoinType.SEMI,
            io.substrait.proto.JoinRel.JoinType.JOIN_TYPE_ANTI, Join.JoinType.ANTI);

    Rel fromProto(io.substrait.proto.Rel protoRel) {
        return switch (protoRel.getRelTypeCase()) {
            case PROJECT -> convertProject(protoRel.getProject());
            case READ -> convertRead(protoRel.getRead());
            case FILTER -> convertFilter(protoRel.getFilter());
            case JOIN -> convertJoin(protoRel.getJoin());
            case SORT -> convertSort(protoRel.getSort());
            case FETCH -> convertFetch(protoRel.getFetch());
            default -> throw unsupported("Rel type: " + protoRel.getRelTypeCase());
        };
    }

    private Rel convertProject(io.substrait.proto.ProjectRel proj) {
        Rel input = fromProto(proj.getInput());
        List<Expression> expressions = proj.getExpressionsList().stream()
                .map(this::convertExpression)
                .toList();
        return ImmutableProject.builder()
                .input(input)
                .expressions(expressions)
                .build();
    }

    private Rel convertRead(io.substrait.proto.ReadRel read) {
        if (read.getReadTypeCase() != io.substrait.proto.ReadRel.ReadTypeCase.VIRTUAL_TABLE) {
            throw unsupported("ReadRel type: " + read.getReadTypeCase());
        }
        var vt = read.getVirtualTable();
        List<String> names = read.hasBaseSchema()
                ? read.getBaseSchema().getNamesList() : List.of();
        List<Expression.StructLiteral> rows = vt.getValuesList().stream()
                .map(this::convertStruct)
                .toList();
        return ImmutableVirtualTableScan.builder()
                .dfsNames(names)
                .addAllRows(rows)
                .build();
    }

    private Rel convertFilter(io.substrait.proto.FilterRel filter) {
        Rel input = fromProto(filter.getInput());
        Expression condition = convertExpression(filter.getCondition());
        return ImmutableFilter.builder()
                .input(input)
                .condition(condition)
                .build();
    }

    private Rel convertJoin(io.substrait.proto.JoinRel join) {
        Rel left = fromProto(join.getLeft());
        Rel right = fromProto(join.getRight());
        Join.JoinType joinType = JOIN_TYPE_MAP.get(join.getType());
        if (joinType == null) {
            throw unsupported("JoinType: " + join.getType());
        }
        var builder = ImmutableJoin.builder()
                .left(left)
                .right(right)
                .joinType(joinType);
        if (join.hasExpression()) {
            builder.condition(convertExpression(join.getExpression()));
        }
        return builder.build();
    }

    private Rel convertSort(io.substrait.proto.SortRel sort) {
        Rel input = fromProto(sort.getInput());
        List<Expression.SortField> sortFields = sort.getSortsList().stream()
                .map(this::convertSortField)
                .toList();
        return ImmutableSort.builder()
                .input(input)
                .sortFields(sortFields)
                .build();
    }

    private Expression.SortField convertSortField(io.substrait.proto.SortField sf) {
        Expression expr = convertExpression(sf.getExpr());
        Expression.SortDirection dir = mapSortDirection(sf.getDirection());
        return ImmutableExpression.SortField.builder()
                .expr(expr)
                .direction(dir)
                .build();
    }

    private static Expression.SortDirection mapSortDirection(
            io.substrait.proto.SortField.SortDirection dir) {
        return switch (dir) {
            case SORT_DIRECTION_ASC_NULLS_FIRST -> Expression.SortDirection.ASC_NULLS_FIRST;
            case SORT_DIRECTION_ASC_NULLS_LAST -> Expression.SortDirection.ASC_NULLS_LAST;
            case SORT_DIRECTION_DESC_NULLS_FIRST -> Expression.SortDirection.DESC_NULLS_FIRST;
            case SORT_DIRECTION_DESC_NULLS_LAST -> Expression.SortDirection.DESC_NULLS_LAST;
            default -> Expression.SortDirection.ASC_NULLS_LAST;
        };
    }

    private Rel convertFetch(io.substrait.proto.FetchRel fetch) {
        Rel input = fromProto(fetch.getInput());
        var builder = ImmutableFetch.builder()
                .input(input)
                .offset(fetch.getOffset());
        if (fetch.getCount() >= 0) {
            builder.count(fetch.getCount());
        }
        return builder.build();
    }

    private Expression.StructLiteral convertStruct(
            io.substrait.proto.Expression.Literal.Struct struct) {
        List<Expression.Literal> fields = struct.getFieldsList().stream()
                .map(this::convertLiteral)
                .toList();
        return ExpressionCreator.struct(false, fields);
    }

    private Expression convertExpression(io.substrait.proto.Expression expr) {
        return switch (expr.getRexTypeCase()) {
            case LITERAL -> convertLiteral(expr.getLiteral());
            case SELECTION -> convertFieldRef(expr.getSelection());
            default -> throw unsupported("Expression type: " + expr.getRexTypeCase());
        };
    }

    private Expression.Literal convertLiteral(io.substrait.proto.Expression.Literal lit) {
        boolean nullable = lit.getNullable();
        return switch (lit.getLiteralTypeCase()) {
            case I32 -> ExpressionCreator.i32(nullable, lit.getI32());
            case I64 -> ExpressionCreator.i64(nullable, lit.getI64());
            case FP64 -> ExpressionCreator.fp64(nullable, lit.getFp64());
            case BOOLEAN -> ExpressionCreator.bool(nullable, lit.getBoolean());
            case STRING -> ExpressionCreator.string(nullable, lit.getString());
            case DECIMAL -> convertDecimal(lit, nullable);
            default -> throw unsupported("Literal type: " + lit.getLiteralTypeCase());
        };
    }

    private Expression.Literal convertDecimal(
            io.substrait.proto.Expression.Literal lit, boolean nullable) {
        var dec = lit.getDecimal();
        BigDecimal value = new BigDecimal(
                new java.math.BigInteger(dec.getValue().toByteArray()), dec.getScale());
        return ExpressionCreator.decimal(nullable, value, dec.getPrecision(), dec.getScale());
    }

    private Expression convertFieldRef(
            io.substrait.proto.Expression.FieldReference ref) {
        if (!ref.hasDirectReference()) {
            throw unsupported("Non-direct field reference");
        }
        var seg = ref.getDirectReference();
        if (!seg.hasStructField()) {
            throw unsupported("Non-struct field reference");
        }
        int index = seg.getStructField().getField();
        Type type = TypeCreator.of(false).I32; // placeholder type for field refs
        return io.substrait.expression.FieldReference.newRootStructReference(index, type);
    }

    private SubstraitConversionException unsupported(String detail) {
        return new SubstraitConversionException(
                PangrovaErrorCode.SUBSTRAIT_UNSUPPORTED_PLAN_NODE,
                "Deserialization not supported: " + detail);
    }
}
