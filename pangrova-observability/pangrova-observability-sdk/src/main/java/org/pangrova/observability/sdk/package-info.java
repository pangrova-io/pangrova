/**
 * OpenTelemetry-backed provider implementations, SDK configuration, and transport adapters.
 *
 * <p>SDK packages are implementation detail: engine modules must depend on
 * {@code org.pangrova.observability.api} for contracts and null objects, and only
 * embedding hosts (Coordinator, Worker, Standalone) may assemble SDK providers.</p>
 */
package org.pangrova.observability.sdk;
