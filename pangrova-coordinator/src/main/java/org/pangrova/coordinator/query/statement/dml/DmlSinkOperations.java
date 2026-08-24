package org.pangrova.coordinator.query.statement.dml;

import org.pangrova.metadata.catalog.CatalogHandle;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.ConnectorTransactionHandle;
import org.pangrova.spi.connector.data.ConnectorPageSink;
import org.pangrova.spi.connector.data.page.Page;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;

/**
 * Encapsulates connector sink write operations for DML paths.
 *
 * <p>Provides batch write operations for INSERT and INSERT OVERWRITE
 * via {@link ConnectorPageSink}. After writing, commits are performed
 * through {@code ConnectorMetadata.finishDistributedWrite()}.</p>
 */
final class DmlSinkOperations {

    private DmlSinkOperations() {}

    static void writePageViaSink(
            CatalogHandle catalogHandle, ConnectorSession session,
            ConnectorOutputTableHandle outputHandle, Page page) {
        ConnectorPageSink sink = catalogHandle.getPageSinkProvider()
                .createPageSink(ConnectorTransactionHandle.EMPTY, session, outputHandle);
        try {
            sink.appendPage(page);
            byte[] commitMetadata = sink.finishWithMetadata();
            catalogHandle.getMetadata(session)
                    .finishDistributedWrite(session, outputHandle, java.util.List.of(commitMetadata));
        } catch (Exception e) {
            sink.abort();
            throw e;
        } finally {
            sink.close();
        }
    }

    static void writePageViaOverwriteSink(
            CatalogHandle catalogHandle, ConnectorSession session,
            ConnectorOutputTableHandle outputHandle, Page page) {
        ConnectorPageSink sink = catalogHandle.getPageSinkProvider()
                .createOverwritePageSink(ConnectorTransactionHandle.EMPTY, session, outputHandle);
        try {
            sink.appendPage(page);
            byte[] commitMetadata = sink.finishWithMetadata();
            catalogHandle.getMetadata(session)
                    .finishDistributedWrite(session, outputHandle, java.util.List.of(commitMetadata));
        } catch (Exception e) {
            sink.abort();
            throw e;
        } finally {
            sink.close();
        }
    }
}
