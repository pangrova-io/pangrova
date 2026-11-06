package org.pangrova.parser.api.ast.statement.dml;

import java.util.List;
import java.util.Optional;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.StatementType;
import org.pangrova.parser.api.ast.TableRef;

/**
 * Represents MERGE INTO target USING source ON condition WHEN MATCHED/NOT MATCHED ...
 */
public record MergeStatement(
        Identifier targetTable,
        Optional<String> targetAlias,
        TableRef sourceTable,
        Expression onCondition,
        List<MergeClause> clauses) implements Statement {

    public MergeStatement {
        clauses = List.copyOf(clauses);
    }

    /** A single WHEN MATCHED/NOT MATCHED clause. */
    public sealed interface MergeClause {

        record WhenMatchedUpdate(
                Optional<Expression> condition,
                List<UpdateStatement.Assignment> assignments) implements MergeClause {
            public WhenMatchedUpdate {
                assignments = List.copyOf(assignments);
            }
        }

        record WhenMatchedDelete(
                Optional<Expression> condition) implements MergeClause {}

        record WhenNotMatchedInsert(
                Optional<Expression> condition,
                Optional<List<String>> columns,
                List<Expression> values) implements MergeClause {
            public WhenNotMatchedInsert {
                columns = columns.map(List::copyOf);
                values = List.copyOf(values);
            }
        }
    }

    @Override
    public StatementType type() {
        return StatementType.MERGE;
    }
}
