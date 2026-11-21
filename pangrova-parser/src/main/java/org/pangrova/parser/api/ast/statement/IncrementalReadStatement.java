package org.pangrova.parser.api.ast.statement;

import java.util.List;
import java.util.Optional;

import org.pangrova.parser.api.ast.Expression;
import org.pangrova.parser.api.ast.SelectItem;
import org.pangrova.parser.api.ast.SortItem;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.enums.StatementType;

/**
 * Represents a {@code SELECT ... FROM table CHANGES BETWEEN SNAPSHOT x AND y} statement.
 *
 * <p>This statement reads incremental changes between two snapshots of a table,
 * returning changed rows with an implicit {@code _change_type} column indicating
 * the type of change (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE).</p>
 *
 * @param selectItems    columns to project
 * @param tableName      fully qualified table name parts
 * @param fromSnapshot   exclusive lower-bound snapshot expression
 * @param toSnapshot     inclusive upper-bound snapshot expression
 * @param where          optional filter on the changed rows
 * @param orderBy        optional ordering
 * @param limit          optional row limit
 */
public record IncrementalReadStatement(
        List<SelectItem> selectItems,
        List<String> tableName,
        Expression fromSnapshot,
        Expression toSnapshot,
        Optional<Expression> where,
        List<SortItem> orderBy,
        Optional<Expression> limit
) implements Statement {

    public IncrementalReadStatement {
        selectItems = List.copyOf(selectItems);
        tableName = List.copyOf(tableName);
        orderBy = List.copyOf(orderBy);
    }

    @Override
    public StatementType type() {
        return StatementType.INCREMENTAL_READ;
    }
}
