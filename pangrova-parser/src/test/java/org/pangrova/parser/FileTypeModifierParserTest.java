package org.pangrova.parser;

import org.junit.jupiter.api.Test;
import org.pangrova.parser.ast.Statement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.type.DataTypeSpec;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@code FILE}, {@code FILE EXTERNAL}, and {@code FILE MANAGED}
 * column declarations. The storage modifier selects whether the row references an
 * existing object or copies its bytes into managed storage; the physical shape is
 * the same five-field struct.
 */
class FileTypeModifierParserTest {

    private final SqlParser parser = new SqlParser();

    @Test
    void plainFileDefaultsToExternalReference() {
        CreateTableStatement create = createTable("CREATE TABLE t (id BIGINT, img FILE)");

        assertThat(create.columns().get(1).type())
                .isEqualTo(DataTypeSpec.FileType.INSTANCE);
        assertThat(create.columns().get(1).type().toCanonicalString()).isEqualTo("FILE");
    }

    @Test
    void explicitExternalModifierIsAccepted() {
        CreateTableStatement create = createTable(
                "CREATE TABLE t (id BIGINT, img FILE EXTERNAL)");

        assertThat(create.columns().get(1).type())
                .isEqualTo(new DataTypeSpec.FileType(DataTypeSpec.FileType.FileStorage.EXTERNAL));
        assertThat(create.columns().get(1).type().toCanonicalString()).isEqualTo("FILE");
    }

    @Test
    void managedModifierIsRecorded() {
        CreateTableStatement create = createTable(
                "CREATE TABLE t (id BIGINT, img FILE MANAGED)");

        assertThat(create.columns().get(1).type())
                .isEqualTo(DataTypeSpec.FileType.MANAGED);
        assertThat(create.columns().get(1).type().toCanonicalString()).isEqualTo("FILE MANAGED");
    }

    @Test
    void modifierIsCaseInsensitive() {
        CreateTableStatement create = createTable(
                "CREATE TABLE t (id BIGINT, img file managed)");

        assertThat(create.columns().get(1).type())
                .isEqualTo(DataTypeSpec.FileType.MANAGED);
    }

    @Test
    void rawTypeStringRoundTripsTheModifier() {
        assertThat(DataTypeSpec.fromRawTypeString("FILE"))
                .isEqualTo(DataTypeSpec.FileType.INSTANCE);
        assertThat(DataTypeSpec.fromRawTypeString("file external"))
                .isEqualTo(DataTypeSpec.FileType.INSTANCE);
        assertThat(DataTypeSpec.fromRawTypeString("FILE MANAGED"))
                .isEqualTo(DataTypeSpec.FileType.MANAGED);
    }

    private CreateTableStatement createTable(String sql) {
        Statement statement = parser.parse(sql);
        assertThat(statement).isInstanceOf(CreateTableStatement.class);
        return (CreateTableStatement) statement;
    }
}
