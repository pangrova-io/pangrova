/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.query.execution.dispatch.task;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementCategory;
import org.pangrova.coordinator.query.execution.dispatch.StatementExecutionContext;
import org.pangrova.coordinator.query.execution.dispatch.StatementResult;
import org.pangrova.coordinator.query.execution.split.SplitDrainer;
import org.pangrova.coordinator.query.statement.TableResolver;
import org.pangrova.metadata.api.catalog.CatalogHandle;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.parser.ast.Expression;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.Literal;
import org.pangrova.parser.ast.LiteralValue;
import org.pangrova.parser.ast.SortItem;
import org.pangrova.parser.ast.statement.IncrementalReadStatement;
import org.pangrova.connector.spi.ConnectorSession;
import org.pangrova.connector.spi.ConnectorTransactionHandle;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.connector.spi.data.page.Page;
import org.pangrova.connector.spi.metadata.Constraint;
import org.pangrova.connector.spi.metadata.ConnectorMetadata;
import org.pangrova.connector.spi.metadata.ConnectorSplit;
import org.pangrova.connector.spi.metadata.DynamicFilter;
import org.pangrova.connector.spi.metadata.table.ColumnHandle;
import org.pangrova.connector.spi.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.spi.metadata.table.QualifiedTableName;

/**
 * Executes {@code SELECT ... FROM table CHANGES BETWEEN SNAPSHOT x AND y}.
 *
 * <p>Resolves the table, calls the connector's incremental split manager
 * to get only the data added between the two snapshots, reads the rows,
 * and adds a synthetic {@code _change_type} column set to "INSERT".</p>
 */
public final class IncrementalReadTask implements MetadataTask<IncrementalReadStatement> {

    private final CatalogRegistry catalogRegistry;

    public IncrementalReadTask(CatalogRegistry catalogRegistry) {
        this.catalogRegistry = catalogRegistry;
    }

    @Override
    public StatementResult execute(IncrementalReadStatement statement, StatementExecutionContext context) {
        QualifiedTableName ref = TableResolver.resolveTableRef(statement.tableName(), context.session());

        long fromSnapshot = extractSnapshotId(statement.fromSnapshot(), "fromSnapshot");
        long toSnapshot = extractSnapshotId(statement.toSnapshot(), "toSnapshot");

        ConnectorSession session = TableResolver.buildSession(context.session(), ref.catalog(), ref.schema());
        CatalogHandle catalogHandle = catalogRegistry.getCatalog(ref.catalog());
        ConnectorMetadata metadata = session.getQueryAuthority()
                .map(org.pangrova.connector.spi.metadata.ConnectorMetadataScope::require)
                .map(authority -> authority.beginMetadata(ref.catalog(), () -> catalogHandle.getMetadata(session)))
                .orElseGet(() -> catalogHandle.getMetadata(session));
        ConnectorTableHandle tableHandle = TableResolver.requireTableHandle(metadata, session, ref);

        if (fromSnapshot == toSnapshot) {
            return new StatementResult(List.of(), List.of(), null, StatementCategory.QUERY);
        }

        var columnHandleMap = metadata.getColumnHandles(session, tableHandle);
        List<ColumnHandle> columnHandles = List.copyOf(columnHandleMap.values());
        List<String> columnNames = List.copyOf(columnHandleMap.keySet());

        var splitSource = catalogHandle.getSplitManager().getIncrementalSplits(
                session, tableHandle, fromSnapshot, toSnapshot, Constraint.EMPTY);

        List<TypedRow> rows = new ArrayList<>();
        var pageSourceProvider = catalogHandle.getPageSourceProvider();
        for (ConnectorSplit split : SplitDrainer.drainAll(splitSource)) {
            var source = pageSourceProvider.createPageSource(
                    ConnectorTransactionHandle.empty(), session, split,
                    tableHandle, columnHandles, DynamicFilter.ALL);
            try {
                while (!source.isFinished()) {
                    Page page = source.getNextPage();
                    if (page != null) {
                        for (TypedRow row : page.rows()) {
                            TypedRow.Builder b = TypedRow.builder();
                            for (String col : columnNames) {
                                ColumnValue val = row.get(col);
                                if (val != null) {
                                    b.put(col, val);
                                }
                            }
                            b.putRaw("_change_type", "INSERT");
                            rows.add(b.build());
                        }
                    }
                }
            } finally {
                source.close();
            }
        }
        splitSource.close();

        // Apply ORDER BY if specified
        if (!statement.orderBy().isEmpty()) {
            rows = applyOrderBy(rows, statement.orderBy());
        }
        // Apply LIMIT if specified
        if (statement.limit().isPresent()) {
            rows = applyLimit(rows, statement.limit().get());
        }

        List<String> resultColumns = new ArrayList<>(columnNames);
        resultColumns.add("_change_type");
        return new StatementResult(rows, resultColumns, null, StatementCategory.QUERY);
    }

    @SuppressWarnings("unchecked")
    private static List<TypedRow> applyOrderBy(List<TypedRow> rows, List<SortItem> orderBy) {
        List<TypedRow> sorted = new ArrayList<>(rows);
        Comparator<TypedRow> comparator = null;
        for (SortItem item : orderBy) {
            String col = extractColumnName(item.expression());
            if (col == null) {
                continue;
            }
            boolean desc = item.direction()
                    .map(d -> d == SortItem.SortDirection.DESC).orElse(false);
            Comparator<TypedRow> c = (a, b) -> {
                Object va = a.getRaw(col);
                Object vb = b.getRaw(col);
                if (va == null && vb == null) return 0;
                if (va == null) return -1;
                if (vb == null) return 1;
                if (va instanceof Comparable && vb instanceof Comparable) {
                    try {
                        return ((Comparable<Object>) va).compareTo(vb);
                    } catch (ClassCastException e) {
                        return va.toString().compareTo(vb.toString());
                    }
                }
                return va.toString().compareTo(vb.toString());
            };
            if (desc) {
                c = c.reversed();
            }
            comparator = (comparator == null) ? c : comparator.thenComparing(c);
        }
        if (comparator != null) {
            sorted.sort(comparator);
        }
        return sorted;
    }

    private static List<TypedRow> applyLimit(List<TypedRow> rows, Expression limitExpr) {
        if (limitExpr instanceof Literal lit
                && lit.value() instanceof LiteralValue.IntegerValue iv) {
            int limit = (int) iv.value();
            if (limit >= 0 && limit < rows.size()) {
                return rows.subList(0, limit);
            }
        }
        return rows;
    }

    private static String extractColumnName(Expression expr) {
        if (expr instanceof Identifier id) {
            List<String> parts = id.parts();
            return parts.get(parts.size() - 1);
        }
        return null;
    }

    private static long extractSnapshotId(Expression expr, String label) {
        if (expr instanceof Literal lit) {
            LiteralValue lv = lit.value();
            if (lv instanceof LiteralValue.IntegerValue iv) {
                return iv.value();
            }
            if (lv instanceof LiteralValue.DecimalValue dv) {
                return dv.value().longValue();
            }
        }
        throw new PangrovaException(PangrovaErrorCode.PLANNER_VALIDATION_FAILURE,
                label + " must be a numeric literal, got: " + expr);
    }
}
