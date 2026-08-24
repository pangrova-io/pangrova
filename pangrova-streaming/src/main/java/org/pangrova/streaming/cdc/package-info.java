/**
 * CDC (Change Data Capture) ingestion: message parsing, record routing,
 * quota enforcement, and stream registration.
 *
 * <p><b>File count governance:</b> 10/15 threshold (as of 2026-05).
 * If this package exceeds 15 files, split into:
 * <ul>
 *   <li>{@code cdc/pipeline/} — CdcPipelineLoop, CdcPipelineConfig,
 *       CdcPipelineLoopContext, CdcQuotaEnforcer, IdempotentExecutor</li>
 *   <li>{@code cdc/registration/} — CdcRegistrationHandler, CdcRegistrationRequest,
 *       CdcRegistrationResponse, CdcStreamInfo</li>
 * </ul></p>
 */
package org.pangrova.streaming.cdc;
