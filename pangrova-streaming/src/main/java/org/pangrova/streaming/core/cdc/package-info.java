/**
 * CDC (Change Data Capture) runtime: message parsing, record routing,
 * quota enforcement, stream registration, and pipeline execution.
 *
 * <p>Contracts live in {@code org.pangrova.streaming.api.cdc}; format providers
 * live in {@code org.pangrova.streaming.provider.cdcformat.*}; commit state
 * lives in {@code org.pangrova.streaming.core.commit}.</p>
 */
package org.pangrova.streaming.core.cdc;
