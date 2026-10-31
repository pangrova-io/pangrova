/**
 * Coordinator-owned shared stage runtime bookkeeping and control lifecycle. Ordinary and durable execution
 * use the same dispatch engine; this package does not compile plans, authorize users or transport result rows.
 */
package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;
