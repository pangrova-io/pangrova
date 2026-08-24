package org.pangrova.parser;

import org.pangrova.parser.ast.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies SQL standard comma-JOIN association: explicit JOIN binds tighter than comma.
 *
 * <p>Per SQL standard, {@code FROM a, b JOIN c ON b.id=c.id} must parse as
 * {@code FROM a CROSS JOIN (b JOIN c ON b.id=c.id)} — the explicit JOIN groups
 * {@code b} and {@code c} together, and the comma cross-joins {@code a} with
 * that group.</p>
 */
class CommaJoinAssociationTest {

    private final SqlParser parser = new SqlParser();

    @Test
    void commaBindsLooserThanExplicitJoin() {
        // FROM a, b JOIN c ON b.id = c.id
        // Expected: tables=[a, b], joins=[INNER JOIN c ON b.id=c.id]
        // Meaning: b JOIN c is grouped first, then cross-joined with a
        Statement stmt = parser.parse("SELECT * FROM a, b JOIN c ON b.id = c.id");
        assertThat(stmt).isInstanceOf(Query.class);
        Query query = (Query) stmt;

        // Two comma-separated groups: 'a' is standalone, 'b' is the base of the join group
        List<TableRef> tables = query.from();
        assertThat(tables).hasSize(2);
        assertThat(tables.get(0).name().parts()).containsExactly("a");
        assertThat(tables.get(1).name().parts()).containsExactly("b");

        // One explicit join: c joins onto b (within the second comma group)
        List<JoinClause> joins = query.joins();
        assertThat(joins).hasSize(1);
        assertThat(joins.get(0).joinType()).isEqualTo(JoinClause.JoinType.INNER);
        assertThat(joins.get(0).right().name().parts()).containsExactly("c");
        assertThat(joins.get(0).condition()).isPresent();
    }

    @Test
    void multipleJoinsWithinSingleCommaGroup() {
        // FROM a, b JOIN c ON b.id = c.id JOIN d ON c.id = d.id
        // Expected: tables=[a, b], joins=[JOIN c, JOIN d]
        Statement stmt = parser.parse(
                "SELECT * FROM a, b JOIN c ON b.id = c.id JOIN d ON c.id = d.id");
        assertThat(stmt).isInstanceOf(Query.class);
        Query query = (Query) stmt;

        assertThat(query.from()).hasSize(2);
        assertThat(query.from().get(0).name().parts()).containsExactly("a");
        assertThat(query.from().get(1).name().parts()).containsExactly("b");

        assertThat(query.joins()).hasSize(2);
        assertThat(query.joins().get(0).right().name().parts()).containsExactly("c");
        assertThat(query.joins().get(1).right().name().parts()).containsExactly("d");
    }

    @Test
    void multipleCommaGroupsEachWithJoins() {
        // FROM a JOIN b ON a.id = b.id, c JOIN d ON c.id = d.id
        // Expected: tables=[a, c], joins=[JOIN b, JOIN d]
        Statement stmt = parser.parse(
                "SELECT * FROM a JOIN b ON a.id = b.id, c JOIN d ON c.id = d.id");
        assertThat(stmt).isInstanceOf(Query.class);
        Query query = (Query) stmt;

        assertThat(query.from()).hasSize(2);
        assertThat(query.from().get(0).name().parts()).containsExactly("a");
        assertThat(query.from().get(1).name().parts()).containsExactly("c");

        assertThat(query.joins()).hasSize(2);
        assertThat(query.joins().get(0).joinType()).isEqualTo(JoinClause.JoinType.INNER);
        assertThat(query.joins().get(0).right().name().parts()).containsExactly("b");
        assertThat(query.joins().get(1).joinType()).isEqualTo(JoinClause.JoinType.INNER);
        assertThat(query.joins().get(1).right().name().parts()).containsExactly("d");
    }

    @Test
    void commaOnlyFromClause() {
        // FROM a, b, c — all comma-separated, no explicit joins
        Statement stmt = parser.parse("SELECT * FROM a, b, c");
        assertThat(stmt).isInstanceOf(Query.class);
        Query query = (Query) stmt;

        assertThat(query.from()).hasSize(3);
        assertThat(query.joins()).isEmpty();
    }

    @Test
    void explicitJoinOnlyFromClause() {
        // FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id
        Statement stmt = parser.parse(
                "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id");
        assertThat(stmt).isInstanceOf(Query.class);
        Query query = (Query) stmt;

        assertThat(query.from()).hasSize(1);
        assertThat(query.from().get(0).name().parts()).containsExactly("a");
        assertThat(query.joins()).hasSize(2);
        assertThat(query.joins().get(0).right().name().parts()).containsExactly("b");
        assertThat(query.joins().get(1).right().name().parts()).containsExactly("c");
    }

    @Test
    void crossJoinInCommaGroup() {
        // FROM a, b CROSS JOIN c
        Statement stmt = parser.parse("SELECT * FROM a, b CROSS JOIN c");
        assertThat(stmt).isInstanceOf(Query.class);
        Query query = (Query) stmt;

        assertThat(query.from()).hasSize(2);
        assertThat(query.from().get(0).name().parts()).containsExactly("a");
        assertThat(query.from().get(1).name().parts()).containsExactly("b");

        assertThat(query.joins()).hasSize(1);
        assertThat(query.joins().get(0).joinType()).isEqualTo(JoinClause.JoinType.CROSS);
        assertThat(query.joins().get(0).right().name().parts()).containsExactly("c");
        assertThat(query.joins().get(0).condition()).isEmpty();
    }

    @Test
    void leftJoinInCommaGroup() {
        // FROM a, b LEFT JOIN c ON b.id = c.id
        Statement stmt = parser.parse("SELECT * FROM a, b LEFT JOIN c ON b.id = c.id");
        assertThat(stmt).isInstanceOf(Query.class);
        Query query = (Query) stmt;

        assertThat(query.from()).hasSize(2);
        assertThat(query.joins()).hasSize(1);
        assertThat(query.joins().get(0).joinType()).isEqualTo(JoinClause.JoinType.LEFT);
        assertThat(query.joins().get(0).right().name().parts()).containsExactly("c");
        assertThat(query.joins().get(0).condition()).isPresent();
    }
}
