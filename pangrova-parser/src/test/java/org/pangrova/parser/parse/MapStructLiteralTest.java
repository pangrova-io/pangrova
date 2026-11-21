package org.pangrova.parser.parse;

import org.pangrova.parser.api.enums.ExpressionType;


import org.pangrova.parser.api.ast.*;
import org.pangrova.parser.api.ast.expression.ArrayConstructor;
import org.pangrova.parser.api.ast.expression.MapConstructor;
import org.pangrova.parser.api.ast.expression.StructConstructor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for MAP and STRUCT/ROW literal constructor syntax (C.6).
 * Covers: MAP[k, v, ...], STRUCT(expr, ...), ROW(name => expr, ...), and nested combinations.
 */
class MapStructLiteralTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    // ── MAP literal ──

    @Test
    void parseMapLiteralWithStringKeys() {
        Statement stmt = parser.parse("SELECT MAP['a', 1, 'b', 2] FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(MapConstructor.class);
        MapConstructor map = (MapConstructor) expr;
        assertThat(map.size()).isEqualTo(2);
        assertThat(map.type()).isEqualTo(ExpressionType.MAP_CONSTRUCTOR);
    }

    @Test
    void parseEmptyMapLiteral() {
        Statement stmt = parser.parse("SELECT MAP[] FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(MapConstructor.class);
        MapConstructor map = (MapConstructor) expr;
        assertThat(map.size()).isEqualTo(0);
    }

    @Test
    void parseMapLiteralWithExpressionValues() {
        Statement stmt = parser.parse("SELECT MAP['x', a + b, 'y', c * 2] FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(MapConstructor.class);
        MapConstructor map = (MapConstructor) expr;
        assertThat(map.size()).isEqualTo(2);
    }

    // ── STRUCT/ROW positional literal ──

    @Test
    void parseStructPositionalLiteral() {
        Statement stmt = parser.parse("SELECT STRUCT(1, 'hello', true) FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(StructConstructor.class);
        StructConstructor struct = (StructConstructor) expr;
        assertThat(struct.fields()).hasSize(3);
        assertThat(struct.fields().get(0).name()).isEmpty();
        assertThat(struct.type()).isEqualTo(ExpressionType.STRUCT_CONSTRUCTOR);
    }

    @Test
    void parseRowPositionalLiteral() {
        Statement stmt = parser.parse("SELECT ROW(42, 'world') FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(StructConstructor.class);
        StructConstructor struct = (StructConstructor) expr;
        assertThat(struct.fields()).hasSize(2);
        assertThat(struct.fields().get(0).name()).isEmpty();
    }

    // ── ROW named literal ──

    @Test
    void parseRowNamedLiteral() {
        Statement stmt = parser.parse("SELECT ROW(a => 1, b => 'x') FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(StructConstructor.class);
        StructConstructor struct = (StructConstructor) expr;
        assertThat(struct.fields()).hasSize(2);
        assertThat(struct.fields().get(0).name()).hasValue("a");
        assertThat(struct.fields().get(0).value()).isInstanceOf(Literal.class);
        assertThat(struct.fields().get(1).name()).hasValue("b");
    }

    @Test
    void parseStructNamedLiteral() {
        Statement stmt = parser.parse("SELECT STRUCT(name => 'Alice', age => 30) FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(StructConstructor.class);
        StructConstructor struct = (StructConstructor) expr;
        assertThat(struct.fields()).hasSize(2);
        assertThat(struct.fields().get(0).name()).hasValue("name");
        assertThat(struct.fields().get(1).name()).hasValue("age");
    }

    // ── Nested combinations ──

    @Test
    void parseMapOfStructLiteral() {
        Statement stmt = parser.parse(
                "SELECT MAP['user1', ROW(name => 'Alice', age => 30), 'user2', ROW(name => 'Bob', age => 25)] FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(MapConstructor.class);
        MapConstructor map = (MapConstructor) expr;
        assertThat(map.size()).isEqualTo(2);
        assertThat(map.value(0)).isInstanceOf(StructConstructor.class);
        assertThat(map.value(1)).isInstanceOf(StructConstructor.class);
    }

    @Test
    void parseArrayOfMapLiteral() {
        Statement stmt = parser.parse(
                "SELECT ARRAY[MAP['a', 1], MAP['b', 2]] FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(ArrayConstructor.class);
        ArrayConstructor arr = (ArrayConstructor) expr;
        assertThat(arr.elements()).hasSize(2);
        assertThat(arr.elements().get(0)).isInstanceOf(MapConstructor.class);
        assertThat(arr.elements().get(1)).isInstanceOf(MapConstructor.class);
    }

    @Test
    void parseStructWithNestedArray() {
        Statement stmt = parser.parse(
                "SELECT ROW(tags => ARRAY['a', 'b'], count => 5) FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(StructConstructor.class);
        StructConstructor struct = (StructConstructor) expr;
        assertThat(struct.fields()).hasSize(2);
        assertThat(struct.fields().get(0).name()).hasValue("tags");
        assertThat(struct.fields().get(0).value()).isInstanceOf(ArrayConstructor.class);
    }

    @Test
    void parseMapWithNestedMapValue() {
        Statement stmt = parser.parse(
                "SELECT MAP['outer', MAP['inner_key', 42]] FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
        Query q = (Query) stmt;
        Expression expr = ((ExpressionItem) q.selectItems().get(0)).expression();
        assertThat(expr).isInstanceOf(MapConstructor.class);
        MapConstructor outer = (MapConstructor) expr;
        assertThat(outer.size()).isEqualTo(1);
        assertThat(outer.value(0)).isInstanceOf(MapConstructor.class);
    }

    // ── MAP/STRUCT as identifier (non-reserved keyword) ──

    @Test
    void mapAsIdentifierStillWorks() {
        Statement stmt = parser.parse("SELECT map FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
    }

    @Test
    void structAsIdentifierStillWorks() {
        Statement stmt = parser.parse("SELECT struct FROM t");
        assertThat(stmt).isInstanceOf(Query.class);
    }
}
