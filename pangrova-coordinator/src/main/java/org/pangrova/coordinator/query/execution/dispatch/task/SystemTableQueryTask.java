package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.Query;
import org.pangrova.parser.ast.SortItem;
import org.pangrova.parser.ast.SystemTableReference;
import org.pangrova.parser.ast.TableRef;
import org.pangrova.parser.ast.expression.BinaryExpression;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ColumnValue;
import org.pangrova.spi.connector.data.TypedRow;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes SELECT queries that reference system tables (e.g., {@code table$snapshots}).
 *
 * <p>When a query's FROM clause contains a {@link SystemTableReference}, this task
 * resolves the base table via the catalog, then delegates to
 * {@link ConnectorMetadata#getSystemTable(ConnectorSession, ConnectorTableHandle, String)}
 * to retrieve the virtual table's columns and rows.</p>
 */
public final class SystemTableQueryTask {

    private static final Logger LOG = LoggerFactory.getLogger(SystemTableQueryTask.class);

    private final CatalogRegistry catalogRegistry;

    public SystemTableQueryTask(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    /**
     * Extracts the first system table reference from a query's FROM clause.
     * Returns empty if no system table reference is present.
     */
    public static Optional<SystemTableReference> extractSystemTableRef(Query query) {
        for (TableRef ref : query.from()) {
            if (ref.isSystemTable()) {
                return ref.systemTableRef();
            }
        }
        return Optional.empty();
    }

    /**
     * Executes the system table query and returns the result.
     */
    public StatementResult execute(Query query, SystemTableReference sysRef,
                                    StatementExecutionContext context) {
        QualifiedTableName tableName = QualifiedTableName.resolve(sysRef.baseTable().parts());
        String systemTableName = sysRef.systemTableName();

        ConnectorSession session = TableResolver.buildSession(tableName.catalog());
        ConnectorMetadata metadata = catalogRegistry.getCatalog(tableName.catalog())
                .getMetadata(session);

        ConnectorTableHandle tableHandle = TableResolver.requireTableHandle(
                metadata, session, tableName);

        ConnectorSystemTable systemTable = metadata.getSystemTable(
                session, tableHandle, systemTableName)
                .orElseThrow(() -> new PangrovaException(PangrovaErrorCode.METADATA_TABLE_NOT_FOUND,
                        "System table not found: " + tableName + "$" + systemTableName));

        List<String> columns = systemTable.columns();
        List<TypedRow> rows = systemTable.rows();

        rows = applyWhereFilter(rows, query);
        rows = applyOrderBy(rows, query);
        rows = applyLimit(rows, query);

        LOG.info("event=system_table_query table={}${} columns={} rowCount={}",
                tableName, systemTableName, columns.size(), rows.size());

        return new StatementResult(rows, columns, null, StatementCategory.QUERY);
    }

    private List<TypedRow> applyWhereFilter(List<TypedRow> rows, Query query) {
        if (query.where().isEmpty()) {
            return rows;
        }
        Expression where = query.where().get();
        return rows.stream()
                .filter(row -> evaluateCondition(row, where))
                .toList();
    }

    private List<TypedRow> applyOrderBy(List<TypedRow> rows, Query query) {
        if (query.orderBy().isEmpty()) {
            return rows;
        }
        List<TypedRow> sorted = new ArrayList<>(rows);
        Comparator<TypedRow> comparator = null;
        for (SortItem sortItem : query.orderBy()) {
            String columnName = extractColumnName(sortItem.expression());
            if (columnName == null) {
                continue;
            }
            boolean descending = sortItem.direction()
                    .map(d -> d == SortItem.SortDirection.DESC)
                    .orElse(false);
            Comparator<TypedRow> itemComparator = (a, b) -> {
                Object valA = extractRawValue(a, columnName);
                Object valB = extractRawValue(b, columnName);
                return compareValues(valA, valB);
            };
            if (descending) {
                itemComparator = itemComparator.reversed();
            }
            comparator = comparator == null ? itemComparator : comparator.thenComparing(itemComparator);
        }
        if (comparator != null) {
            sorted.sort(comparator);
        }
        return sorted;
    }

    private List<TypedRow> applyLimit(List<TypedRow> rows, Query query) {
        if (query.limit().isEmpty()) {
            return rows;
        }
        Expression limitExpr = query.limit().get();
        if (limitExpr instanceof Literal lit && lit.value() instanceof LiteralValue.IntegerValue iv) {
            int limit = (int) iv.value();
            if (limit >= 0 && limit < rows.size()) {
                return rows.subList(0, limit);
            }
        }
        return rows;
    }

    private boolean evaluateCondition(TypedRow row, Expression expr) {
        if (expr instanceof BinaryExpression bin) {
            if (bin.operator() == BinaryExpression.Operator.AND) {
                return evaluateCondition(row, bin.left()) && evaluateCondition(row, bin.right());
            }
            if (bin.operator() == BinaryExpression.Operator.OR) {
                return evaluateCondition(row, bin.left()) || evaluateCondition(row, bin.right());
            }
            if (bin.operator() == BinaryExpression.Operator.EQUAL) {
                Object left = resolveValue(row, bin.left());
                Object right = resolveValue(row, bin.right());
                return left != null && left.equals(right);
            }
            if (bin.operator() == BinaryExpression.Operator.NOT_EQUAL) {
                Object left = resolveValue(row, bin.left());
                Object right = resolveValue(row, bin.right());
                return left != null && !left.equals(right);
            }
            if (bin.operator() == BinaryExpression.Operator.GREATER_THAN
                    || bin.operator() == BinaryExpression.Operator.LESS_THAN
                    || bin.operator() == BinaryExpression.Operator.GREATER_THAN_OR_EQUAL
                    || bin.operator() == BinaryExpression.Operator.LESS_THAN_OR_EQUAL) {
                Object left = resolveValue(row, bin.left());
                Object right = resolveValue(row, bin.right());
                int cmp = compareValues(left, right);
                return switch (bin.operator()) {
                    case GREATER_THAN -> cmp > 0;
                    case LESS_THAN -> cmp < 0;
                    case GREATER_THAN_OR_EQUAL -> cmp >= 0;
                    case LESS_THAN_OR_EQUAL -> cmp <= 0;
                    default -> false;
                };
            }
        }
        return true;
    }

    private Object resolveValue(TypedRow row, Expression expr) {
        if (expr instanceof Identifier id) {
            String colName = id.parts().get(id.parts().size() - 1);
            return extractRawValue(row, colName);
        }
        if (expr instanceof Literal lit) {
            LiteralValue lv = lit.value();
            if (lv instanceof LiteralValue.IntegerValue iv) {
                return iv.value();
            }
            if (lv instanceof LiteralValue.StringValue sv) {
                return sv.value();
            }
            if (lv instanceof LiteralValue.BooleanValue bv) {
                return bv.value();
            }
            if (lv instanceof LiteralValue.DecimalValue dv) {
                return dv.value();
            }
            if (lv instanceof LiteralValue.NullValue) {
                return null;
            }
            return lv.toString();
        }
        return null;
    }

    private Object extractRawValue(TypedRow row, String columnName) {
        ColumnValue cv = row.get(columnName);
        return cv != null ? ColumnValue.toRaw(cv) : null;
    }

    private String extractColumnName(Expression expr) {
        if (expr instanceof Identifier id) {
            return id.parts().get(id.parts().size() - 1);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private int compareValues(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        if (a instanceof Comparable && b instanceof Comparable) {
            try {
                return ((Comparable<Object>) a).compareTo(b);
            } catch (ClassCastException e) {
                return a.toString().compareTo(b.toString());
            }
        }
        return a.toString().compareTo(b.toString());
    }
}
