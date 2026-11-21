package org.pangrova.parser.parse;

import org.pangrova.parser.api.exception.ParseException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real grammar/source-span tests, not connector or query execution evidence. */
class TimeTravelSqlClausesTest {
    @Test
    void preservesLiteralsCommentsQuotedNamesAndNonBmpCharacters() {
        String sql = "SELECT '😀 FOR VERSION AS OF 7' FROM \"a.b\" FOR VERSION AS OF 7 AS t "
                + "/* 😀 FOR VERSION AS OF 7 */ WHERE t.id = 1";
        var parsed = TimeTravelSqlClauses.extract(sql);
        assertThat(parsed.strippedSql()).isEqualTo("SELECT '😀 FOR VERSION AS OF 7' FROM \"a.b\"  AS t "
                + "/* 😀 FOR VERSION AS OF 7 */ WHERE t.id = 1");
        assertThat(parsed.references()).singleElement().satisfies(reference -> {
            assertThat(reference.table().name().parts()).containsExactly("a.b");
            assertThat(reference.table().alias()).contains("t");
            assertThat(reference.cte()).isFalse();
        });
    }

    @Test
    void removesActualSystemVersionTimeAndRefSpansRatherThanMatchingLiteralText() {
        String sql = "SELECT 'VERSION AS OF TAG ''x''' FROM a FOR SYSTEM_VERSION AS OF 3 "
                + "JOIN b FOR SYSTEM_TIME AS OF 4 ON a.id = b.id "
                + "JOIN c VERSION AS OF TAG 'x' ON b.id = c.id";
        assertThat(TimeTravelSqlClauses.extract(sql).strippedSql()).isEqualTo(
                "SELECT 'VERSION AS OF TAG ''x''' FROM a  JOIN b  ON a.id = b.id JOIN c  ON b.id = c.id");
    }

    @Test
    void cteNamesShadowOnlyVisibleUnqualifiedReferences() {
        String sql = "WITH rows AS (SELECT id FROM rows FOR VERSION AS OF 2), "
                + "next_rows AS (SELECT id FROM rows) SELECT id FROM next_rows "
                + "UNION ALL SELECT id FROM cat.db.rows";
        var refs = TimeTravelSqlClauses.extract(sql).references();
        assertThat(refs).hasSize(4);
        assertThat(refs.stream().map(TimeTravelSqlClauses.Reference::cte)).containsExactly(false, true, true, false);
        assertThat(refs.get(0).table().timeTravelSpec()).isPresent();
    }

    @Test
    void recursiveSelfReferenceIsLogicalNotAnotherPhysicalTable() {
        var refs = TimeTravelSqlClauses.extract(
                "WITH RECURSIVE rows AS (SELECT id FROM rows) SELECT id FROM rows").references();
        assertThat(refs).hasSize(2).allSatisfy(reference -> assertThat(reference.cte()).isTrue());
    }

    @Test
    void capturesScalarSubqueryAndInsertSelectSetSources() {
        var parsed = TimeTravelSqlClauses.extract("INSERT INTO target SELECT "
                + "(SELECT id FROM history FOR VERSION AS OF 2) FROM current_rows "
                + "UNION ALL SELECT id FROM other FOR VERSION AS OF 4");
        assertThat(parsed.references().stream().map(ref -> ref.table().name().parts().get(0)))
                .containsExactly("history", "current_rows", "other");
        assertThat(parsed.strippedSql()).isEqualTo("INSERT INTO target SELECT "
                + "(SELECT id FROM history ) FROM current_rows UNION ALL SELECT id FROM other ");
    }

    @Test
    void sqlWithoutSourceClausesRetainsEveryOriginalCharacter() {
        String sql = " SELECT '😀 FOR VERSION AS OF 2' /* FOR VERSION AS OF 2 */ ; ";
        assertThat(TimeTravelSqlClauses.extract(sql).strippedSql()).isEqualTo(sql);
    }

    @Test
    void vectorInsertWithScientificCoordinatesRemainsValidSql() {
        String sql = "INSERT INTO vectors VALUES (1, ARRAY[3.3283234E-4,-6.222725e-5,1E+3])";
        assertThat(TimeTravelSqlClauses.extract(sql).strippedSql()).isEqualTo(sql);
        assertThat(new AntlrSqlParser().parse(sql)).isNotNull();
    }

    @Test
    void quotedCteSpellingIsPreservedWhileCurrentPlannerMatchingIsCaseInsensitive() {
        var refs = TimeTravelSqlClauses.extract("WITH \"Rows\" AS (SELECT id FROM source) "
                + "SELECT id FROM \"Rows\" UNION ALL SELECT id FROM rows").references();
        assertThat(refs.stream().map(TimeTravelSqlClauses.Reference::cte)).containsExactly(false, true, true);
        assertThat(refs.get(1).table().name().parts()).containsExactly("Rows");
        assertThat(refs.get(2).table().name().parts()).containsExactly("rows");
    }

    @Test
    void unquotedCteSpellingUsesTheSameCurrentCaseInsensitiveMatchingAsQuotedReferences() {
        var refs = TimeTravelSqlClauses.extract("WITH ROWS AS (SELECT id FROM source) "
                + "SELECT id FROM rows UNION ALL SELECT id FROM \"rows\"").references();
        assertThat(refs.stream().map(TimeTravelSqlClauses.Reference::cte)).containsExactly(false, true, true);
    }

    @Test
    void invalidClauseFailsStrictlyRatherThanReturningCurrentSql() {
        assertThatThrownBy(() -> TimeTravelSqlClauses.extract("SELECT id FROM rows FOR VERSION AS OF"))
                .isInstanceOf(ParseException.class);
    }

    @Test
    void oneOptionalTerminatorIsAcceptedButMultipleStatementsAreRejected() {
        assertThat(new AntlrSqlParser().parse("SELECT 1;")).isNotNull();
        assertThat(TimeTravelSqlClauses.extract("SELECT 1;").strippedSql()).isEqualTo("SELECT 1;");
        assertThatThrownBy(() -> TimeTravelSqlClauses.extract("SELECT 1; SELECT 2"))
                .isInstanceOf(ParseException.class);
        assertThatThrownBy(() -> new AntlrSqlParser().parse("SELECT 1; SELECT 2"))
                .isInstanceOf(ParseException.class);
    }
}
