package org.pangrova.parser.syntax;


import org.pangrova.parser.api.ast.Query;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.parser.api.enums.BaseType;
import org.pangrova.parser.api.ast.type.DataTypeSpec;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies BLOB column type and BLOB function syntax:
 * {@code BLOB}, {@code FROM_FILE(path)}, {@code TO_FILE(blob, path)},
 * {@code BLOB_SIZE(blob)}, {@code BLOB_MIME_TYPE(blob)}.
 */
class BlobTypeSyntaxTest {

    private final AntlrSqlParser parser = new AntlrSqlParser();

    @Test
    void createTableWithBlobColumn() {
        Statement stmt = parser.parse(
                "CREATE TABLE product_images (id BIGINT, image BLOB, caption VARCHAR)");
        assertThat(stmt).isInstanceOf(CreateTableStatement.class);
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns()).hasSize(3);
        assertThat(create.columns().get(1).name()).isEqualTo("image");
        assertThat(create.columns().get(1).type()).isEqualTo(
                new org.pangrova.parser.api.ast.type.DataTypeSpec.PrimitiveType(
                        org.pangrova.parser.api.enums.BaseType.BLOB));
    }

    @Test
    void insertWithFromFile() {
        Statement stmt = parser.parse(
                "INSERT INTO product_images VALUES (1, FROM_FILE('s3://uploads/shoe.jpg'), 'Red shoe')");
        assertThat(stmt).isNotNull();
    }

    @Test
    void selectBlobSizeAndMimeType() {
        Statement stmt = parser.parse(
                "SELECT BLOB_SIZE(image), BLOB_MIME_TYPE(image) FROM product_images");
        assertThat(stmt).isInstanceOf(Query.class);
    }

    @Test
    void selectToFile() {
        Statement stmt = parser.parse(
                "SELECT TO_FILE(image, 's3://exports/product.jpg') FROM product_images WHERE id = 1");
        assertThat(stmt).isInstanceOf(Query.class);
    }

    @Test
    void blobAsIdentifierInSelect() {
        Statement stmt = parser.parse("SELECT blob FROM t");
        assertThat(stmt).isNotNull();
    }

    @Test
    void blobColumnCaseInsensitive() {
        Statement stmt = parser.parse(
                "CREATE TABLE t (id BIGINT, data blob)");
        CreateTableStatement create = (CreateTableStatement) stmt;
        assertThat(create.columns().get(1).type()).isEqualTo(
                new org.pangrova.parser.api.ast.type.DataTypeSpec.PrimitiveType(
                        org.pangrova.parser.api.enums.BaseType.BLOB));
    }

    @Test
    void varbinaryColumnAndNullCastUseCanonicalType() {
        CreateTableStatement create = (CreateTableStatement) parser.parse(
                "CREATE TABLE files (id BIGINT, content VARBINARY)");
        assertThat(create.columns().get(1).type())
                .isEqualTo(new DataTypeSpec.PrimitiveType(BaseType.VARBINARY));
        assertThat(parser.parse("SELECT CAST(NULL AS VARBINARY) AS content"))
                .isInstanceOf(Query.class);
    }
}
