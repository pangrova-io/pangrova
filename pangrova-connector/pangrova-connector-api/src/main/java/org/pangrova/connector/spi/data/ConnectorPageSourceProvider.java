package org.pangrova.connector.spi.data;

import java.util.List;

import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.transaction.ConnectorTransactionHandle;
import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.DynamicFilter;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.common.api.annotation.Stable;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;

/**
 * Creates {@link ConnectorPageSource} instances for reading data from splits.
 *
 * <p>The engine calls {@link #createPageSource} for each split to obtain a
 * source that streams data from the underlying data store.</p>
 */
@Stable(since = "1.0")
public interface ConnectorPageSourceProvider {

    /**
     * Creates a page source for reading data from the specified split.
     *
     * @param transaction   the current transaction handle
     * @param session       the current session context
     * @param split         the split to read data from
     * @param table         the table handle
     * @param columns       the columns to read
     * @param dynamicFilter runtime dynamic filter
     * @return a page source for reading data, never null
     */
    ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter);
}
