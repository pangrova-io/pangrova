/**
 * Observability API surface: profiling contracts and API-owned no-op implementations.
 *
 * <p>Types in this package must not reference the observability SDK, OpenTelemetry,
 * or any exporter. Engine modules that only need null-object behaviour depend on this
 * module instead of the full SDK.</p>
 */
package org.pangrova.observability.api;
