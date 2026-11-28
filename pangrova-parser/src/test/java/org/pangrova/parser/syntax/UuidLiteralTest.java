package org.pangrova.parser.syntax;

import org.pangrova.parser.api.enums.LiteralType;
import org.pangrova.parser.api.exception.ParseException;
import org.pangrova.parser.format.AstPrinter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.parser.api.ast.ExpressionItem;
import org.pangrova.parser.api.ast.Literal;
import org.pangrova.parser.api.ast.LiteralValue;
import org.pangrova.parser.api.ast.Query;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UuidLiteralTest {
    private final AntlrSqlParser parser = new AntlrSqlParser();

    @ParameterizedTest
    @ValueSource(strings = {"00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff", "00112233-4455-6677-8899-AABBCCDDEEFF"})
    void typedUuidLiteralsSurviveTheNeutralAstPrintAndParse(String text) {
        Query query = (Query) parser.parse("SELECT UUID '" + text + "' AS id");
        var literal = (Literal) ((ExpressionItem) query.selectItems().get(0)).expression();
        assertThat(literal.literalType()).isEqualTo(LiteralType.UUID);
        assertThat(literal.value()).isEqualTo(new LiteralValue.UuidValue(UUID.fromString(text)));
        Query restored = (Query) parser.parse(new AstPrinter().print(query));
        assertThat(((Literal) ((ExpressionItem) restored.selectItems().get(0)).expression()).value())
                .isEqualTo(literal.value());
    }

    @ParameterizedTest
    @ValueSource(strings = {"invalid", "00112233-4455-6677-8899-aabbccddeeff00"})
    void invalidUuidTextFailsAsAParseError(String text) {
        assertThatThrownBy(() -> parser.parse("SELECT UUID '" + text + "'"))
                .isInstanceOf(ParseException.class).hasMessageContaining("Invalid UUID literal");
    }

    @Test
    void uuidRemainsAnIdentifierAndAnExplicitDdlOrCastType() {
        assertThat(parser.parse("SELECT uuid FROM uuid")).isInstanceOf(Query.class);
        assertThat(parser.parse("SELECT CAST(NULL AS UUID)")).isInstanceOf(Query.class);
        assertThat(parser.parse("CREATE TABLE t (id UUID DEFAULT UUID '00112233-4455-6677-8899-aabbccddeeff')"))
                .isNotNull();
    }

    @Test
    void sourceSelectorAnalysisPreservesTheUuidLiteralAndSelectedTable() {
        String sql = "SELECT UUID '00112233-4455-6677-8899-aabbccddeeff' FROM uuid FOR VERSION AS OF 2";
        var source = TimeTravelSqlClauses.extract(sql);
        assertThat(source.strippedSql()).isEqualTo(sql.substring(0, sql.indexOf("FOR VERSION")));
        assertThat(source.references()).singleElement().satisfies(reference -> {
            assertThat(reference.table().name().parts()).containsExactly("uuid");
            assertThat(reference.table().timeTravelSpec()).isPresent();
        });
    }
}
