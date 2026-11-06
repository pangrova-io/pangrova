package org.pangrova.parser.core.parse;


import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.type.BaseType;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.parser.api.ast.type.FieldSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies MAP and STRUCT complex type syntax in column definitions,
 * including nested complex types (ARRAY of MAP, STRUCT with ARRAY field).
 */
class ComplexTypeSyntaxTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void createTableWithMapColumn() {
        Statement stmt = parser.parse(
                "CREATE TABLE t (id BIGINT, metadata MAP(VARCHAR, INT))");
        assertThat(stmt).isInstanceOf(CreateTableStatement.class);
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns()).hasSize(2);
        assertThat(create.columns().get(1).name()).isEqualTo("metadata");
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.MapType(
                        new DataTypeSpec.PrimitiveType(BaseType.VARCHAR),
                        new DataTypeSpec.PrimitiveType(BaseType.INTEGER)));
    }

    @Test
    void createTableWithStructColumn() {
        Statement stmt = parser.parse(
                "CREATE TABLE t (id BIGINT, address STRUCT(street VARCHAR, city VARCHAR, zip INT))");
        assertThat(stmt).isInstanceOf(CreateTableStatement.class);
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns()).hasSize(2);
        assertThat(create.columns().get(1).name()).isEqualTo("address");
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.StructType(List.of(
                        new FieldSpec("street", new DataTypeSpec.PrimitiveType(BaseType.VARCHAR)),
                        new FieldSpec("city", new DataTypeSpec.PrimitiveType(BaseType.VARCHAR)),
                        new FieldSpec("zip", new DataTypeSpec.PrimitiveType(BaseType.INTEGER)))));
    }

    @Test
    void createTableWithNestedArrayOfMap() {
        Statement stmt = parser.parse(
                "CREATE TABLE t (id BIGINT, tags ARRAY(MAP(VARCHAR, VARCHAR)))");
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns().get(1).name()).isEqualTo("tags");
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.ArrayType(
                        new DataTypeSpec.MapType(
                                new DataTypeSpec.PrimitiveType(BaseType.VARCHAR),
                                new DataTypeSpec.PrimitiveType(BaseType.VARCHAR))));
    }

    @Test
    void createTableWithStructContainingArray() {
        Statement stmt = parser.parse(
                "CREATE TABLE t (id BIGINT, profile STRUCT(name VARCHAR, scores ARRAY(DOUBLE)))");
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns().get(1).name()).isEqualTo("profile");
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.StructType(List.of(
                        new FieldSpec("name", new DataTypeSpec.PrimitiveType(BaseType.VARCHAR)),
                        new FieldSpec("scores", new DataTypeSpec.ArrayType(
                                new DataTypeSpec.PrimitiveType(BaseType.DOUBLE))))));
    }

    @Test
    void createTableWithMapOfComplexValue() {
        Statement stmt = parser.parse(
                "CREATE TABLE t (id BIGINT, data MAP(VARCHAR, ARRAY(INT)))");
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns().get(1).name()).isEqualTo("data");
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.MapType(
                        new DataTypeSpec.PrimitiveType(BaseType.VARCHAR),
                        new DataTypeSpec.ArrayType(
                                new DataTypeSpec.PrimitiveType(BaseType.INTEGER))));
    }

    @Test
    void createTableWithDeeplyNestedTypes() {
        Statement stmt = parser.parse(
                "CREATE TABLE t (id BIGINT, nested MAP(VARCHAR, STRUCT(items ARRAY(INT), label VARCHAR)))");
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns().get(1).name()).isEqualTo("nested");
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.MapType(
                        new DataTypeSpec.PrimitiveType(BaseType.VARCHAR),
                        new DataTypeSpec.StructType(List.of(
                                new FieldSpec("items", new DataTypeSpec.ArrayType(
                                        new DataTypeSpec.PrimitiveType(BaseType.INTEGER))),
                                new FieldSpec("label", new DataTypeSpec.PrimitiveType(BaseType.VARCHAR))))));
    }

    @Test
    void alterTableAddMapColumn() {
        Statement stmt = parser.parse(
                "ALTER TABLE t ADD COLUMN props MAP(VARCHAR, VARCHAR)");
        assertThat(stmt).isInstanceOf(AlterTableStatement.AddColumn.class);
        AlterTableStatement.AddColumn add = (AlterTableStatement.AddColumn) stmt;
        assertThat(add.columnType()).isEqualTo(
                new DataTypeSpec.MapType(
                        new DataTypeSpec.PrimitiveType(BaseType.VARCHAR),
                        new DataTypeSpec.PrimitiveType(BaseType.VARCHAR)));
    }

    @Test
    void alterTableAddStructColumn() {
        Statement stmt = parser.parse(
                "ALTER TABLE t ADD COLUMN addr STRUCT(city VARCHAR, zip INT)");
        assertThat(stmt).isInstanceOf(AlterTableStatement.AddColumn.class);
        AlterTableStatement.AddColumn add = (AlterTableStatement.AddColumn) stmt;
        assertThat(add.columnType()).isEqualTo(
                new DataTypeSpec.StructType(List.of(
                        new FieldSpec("city", new DataTypeSpec.PrimitiveType(BaseType.VARCHAR)),
                        new FieldSpec("zip", new DataTypeSpec.PrimitiveType(BaseType.INTEGER)))));
    }

    @Test
    void alterTableChangeColumnToMap() {
        Statement stmt = parser.parse(
                "ALTER TABLE t ALTER COLUMN meta TYPE MAP(VARCHAR, BIGINT)");
        assertThat(stmt).isInstanceOf(AlterTableStatement.ChangeColumnType.class);
        AlterTableStatement.ChangeColumnType change = (AlterTableStatement.ChangeColumnType) stmt;
        assertThat(change.newType()).isEqualTo(
                new DataTypeSpec.MapType(
                        new DataTypeSpec.PrimitiveType(BaseType.VARCHAR),
                        new DataTypeSpec.PrimitiveType(BaseType.BIGINT)));
    }

    @Test
    void mapAsIdentifierInSelect() {
        Statement stmt = parser.parse("SELECT map FROM t");
        assertThat(stmt).isNotNull();
    }

    @Test
    void structAsIdentifierInSelect() {
        Statement stmt = parser.parse("SELECT struct FROM t");
        assertThat(stmt).isNotNull();
    }

    @Test
    void castToMapType() {
        Statement stmt = parser.parse("SELECT CAST(x AS MAP(VARCHAR, INT)) FROM t");
        assertThat(stmt).isNotNull();
    }

    @Test
    void castToStructType() {
        Statement stmt = parser.parse("SELECT CAST(x AS STRUCT(name VARCHAR, age INT)) FROM t");
        assertThat(stmt).isNotNull();
    }

    @Test
    void mapColumnCaseInsensitive() {
        Statement stmt = parser.parse(
                "CREATE TABLE t (id BIGINT, m map(varchar, int))");
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.MapType(
                        new DataTypeSpec.PrimitiveType(BaseType.VARCHAR),
                        new DataTypeSpec.PrimitiveType(BaseType.INTEGER)));
    }

    @Test
    void structColumnCaseInsensitive() {
        Statement stmt = parser.parse(
                "CREATE TABLE t (id BIGINT, s struct(name varchar, age int))");
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.StructType(List.of(
                        new FieldSpec("name", new DataTypeSpec.PrimitiveType(BaseType.VARCHAR)),
                        new FieldSpec("age", new DataTypeSpec.PrimitiveType(BaseType.INTEGER)))));
    }
}
