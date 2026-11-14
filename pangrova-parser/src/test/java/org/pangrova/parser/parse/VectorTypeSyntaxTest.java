package org.pangrova.parser.parse;

import org.pangrova.parser.api.exception.ParseException;

import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.pangrova.parser.api.enums.DistanceMetric;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies VECTOR type syntax in column definitions:
 * {@code VECTOR(dimension)} and {@code VECTOR(dimension, FLOAT|DOUBLE)}.
 */
class VectorTypeSyntaxTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void createTableWithVectorColumnDefaultFloat() {
        Statement stmt = parser.parse(
                "CREATE TABLE embeddings (id BIGINT, embedding VECTOR(768))");
        assertThat(stmt).isInstanceOf(CreateTableStatement.class);
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns()).hasSize(2);
        assertThat(create.columns().get(1).name()).isEqualTo("embedding");
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.VectorType(768, DistanceMetric.COSINE));
    }

    @Test
    void createTableWithVectorColumnExplicitFloat() {
        Statement stmt = parser.parse(
                "CREATE TABLE embeddings (id BIGINT, embedding VECTOR(384, FLOAT))");
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.VectorType(384, DistanceMetric.COSINE));
    }

    @Test
    void createTableWithVectorColumnDouble() {
        Statement stmt = parser.parse(
                "CREATE TABLE embeddings (id BIGINT, embedding VECTOR(384, DOUBLE))");
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.VectorType(384, DistanceMetric.COSINE,
                        org.pangrova.types.enums.VectorElementType.DOUBLE));
    }

    @Test
    void createTableWithVectorColumnCaseInsensitive() {
        Statement stmt = parser.parse(
                "CREATE TABLE embeddings (id BIGINT, embedding vector(128))");
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns().get(1).type()).isEqualTo(
                new DataTypeSpec.VectorType(128, DistanceMetric.COSINE));
    }

    @Test
    void alterTableAddVectorColumn() {
        Statement stmt = parser.parse(
                "ALTER TABLE embeddings ADD COLUMN vec VECTOR(768)");
        assertThat(stmt).isInstanceOf(AlterTableStatement.AddColumn.class);
        AlterTableStatement.AddColumn add = (AlterTableStatement.AddColumn) stmt;
        assertThat(add.columnType()).isEqualTo(
                new DataTypeSpec.VectorType(768, DistanceMetric.COSINE));
    }

    @Test
    void alterTableChangeColumnToVector() {
        Statement stmt = parser.parse(
                "ALTER TABLE embeddings ALTER COLUMN vec TYPE VECTOR(512, DOUBLE)");
        assertThat(stmt).isInstanceOf(AlterTableStatement.ChangeColumnType.class);
        AlterTableStatement.ChangeColumnType change = (AlterTableStatement.ChangeColumnType) stmt;
        assertThat(change.newType()).isEqualTo(
                new DataTypeSpec.VectorType(512, DistanceMetric.COSINE,
                        org.pangrova.types.enums.VectorElementType.DOUBLE));
    }

    @Test
    void vectorWithoutDimensionFails() {
        assertThatThrownBy(() -> parser.parse(
                "CREATE TABLE t (v VECTOR)"))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void vectorWithInvalidElementTypeFails() {
        assertThatThrownBy(() -> parser.parse(
                "CREATE TABLE t (v VECTOR(128, INT))"))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void vectorAsIdentifierInSelect() {
        Statement stmt = parser.parse("SELECT vector FROM t");
        assertThat(stmt).isNotNull();
    }
}
