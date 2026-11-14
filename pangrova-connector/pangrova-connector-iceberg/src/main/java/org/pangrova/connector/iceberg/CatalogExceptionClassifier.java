package org.pangrova.connector.iceberg;

import org.pangrova.connector.iceberg.enums.CatalogFailureCategory;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.connector.api.exception.ConnectorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

/**
 * Classifies exceptions from Iceberg catalog RPC calls and routes them
 * to appropriate handling: IOException → retry, AccessDenied → rethrow,
 * other → wrap as ConnectorException.
 *
 * <p>Exposes failure counters for observability. Replaces catch-all silent
 * degradation with explicit exception routing.
 */
final class CatalogExceptionClassifier {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogExceptionClassifier.class);

    private final LongAdder ioFailureCount = new LongAdder();
    private final LongAdder accessDeniedCount = new LongAdder();
    private final LongAdder otherFailureCount = new LongAdder();


    /**
     * Classifies the given exception into a routing category.
     */
    CatalogFailureCategory classify(Exception e) {
        if (isAccessDenied(e)) {
            return CatalogFailureCategory.ACCESS_DENIED;
        }
        if (isIoTransient(e)) {
            return CatalogFailureCategory.IO_TRANSIENT;
        }
        return CatalogFailureCategory.OTHER;
    }

    /**
     * Handles a catalog RPC exception by classifying it and either rethrowing
     * (for access denied) or wrapping it as a ConnectorException with the
     * appropriate error code.
     *
     * @param e         the exception from the catalog call
     * @param operation description of the operation for logging
     * @param tableId   table identifier for context
     * @return a ConnectorException for IO_TRANSIENT/OTHER cases
     * @throws ConnectorException always for ACCESS_DENIED
     */
    ConnectorException handleAndClassify(Exception e, String operation, String tableId) {
        CatalogFailureCategory category = classify(e);
        switch (category) {
            case ACCESS_DENIED -> {
                accessDeniedCount.increment();
                LOG.warn("event=catalog_access_denied operation={} table={} error={}",
                        operation, tableId, e.getMessage());
                throw new ConnectorException(
                        PangrovaErrorCode.CONNECTOR_PERMISSION_DENIED,
                        "Access denied during %s for table %s: %s"
                                .formatted(operation, tableId, e.getMessage()),
                        e);
            }
            case IO_TRANSIENT -> {
                ioFailureCount.increment();
                LOG.warn("event=catalog_io_failure operation={} table={} error={}",
                        operation, tableId, e.getMessage());
                return new ConnectorException(
                        PangrovaErrorCode.CONNECTOR_TRANSIENT_ERROR,
                        "Transient I/O failure during %s for table %s: %s"
                                .formatted(operation, tableId, e.getMessage()),
                        e);
            }
            default -> {
                otherFailureCount.increment();
                LOG.warn("event=catalog_rpc_failed operation={} table={} error={}",
                        operation, tableId, e.getMessage());
                return new ConnectorException(
                        PangrovaErrorCode.CONNECTOR_READ_ERROR,
                        "Catalog RPC failed during %s for table %s: %s"
                                .formatted(operation, tableId, e.getMessage()),
                        e);
            }
        }
    }

    /** Total I/O transient failures observed. */
    long ioFailureTotal() {
        return ioFailureCount.sum();
    }

    /** Total access denied failures observed. */
    long accessDeniedTotal() {
        return accessDeniedCount.sum();
    }

    /** Total other (unclassified) failures observed. */
    long otherFailureTotal() {
        return otherFailureCount.sum();
    }

    /** Combined total of all catalog RPC failures. */
    long totalFailures() {
        return ioFailureCount.sum() + accessDeniedCount.sum() + otherFailureCount.sum();
    }

    private static boolean isAccessDenied(Throwable t) {
        while (t != null) {
            if (hasDeniedClassName(t.getClass().getSimpleName())
                    || hasDeniedMessage(t.getMessage())) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private static boolean hasDeniedClassName(String name) {
        return name.contains("AccessDenied") || name.contains("Forbidden")
                || name.contains("UnauthorizedException")
                || name.contains("NotAuthorizedException");
    }

    private static boolean hasDeniedMessage(String msg) {
        return msg != null && (msg.contains("Access Denied") || msg.contains("403"));
    }

    private static boolean isIoTransient(Throwable t) {
        while (t != null) {
            if (t instanceof IOException) {
                return true;
            }
            String name = t.getClass().getSimpleName();
            if (name.contains("ServiceUnavailable") || name.contains("ConnectionReset")
                    || name.contains("SocketTimeout") || name.contains("ConnectTimeout")) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
