/**
 * Internal durable exchange evidence ports owned by pangrova-coordinator.
 * The shared DAG lifecycle may observe actual allocation/finalize calls here;
 * row transport, stored credentials and independent owner authority are prohibited.
 */
package org.pangrova.coordinator.query.execution.dispatch.shuffle.databuild;
