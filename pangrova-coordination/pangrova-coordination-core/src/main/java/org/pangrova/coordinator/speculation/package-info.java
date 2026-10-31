/**
 * Speculative task dispatch and logical-partition winner selection.
 * A copy is registered only when a real worker dispatch is configured;
 * task reports elect one winner before write fragments or DAG completion
 * are accepted, and losing attempts are canceled without failing the query.
 */
package org.pangrova.coordinator.speculation;
