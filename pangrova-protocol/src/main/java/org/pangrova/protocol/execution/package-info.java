/**
 * Immutable execution-plan and task control values shared across process adapters.
 * Does not own scheduling, execution state transitions, or durable query authority.
 *
 * <p>Owned by pangrova-protocol; consumed by engine, connector and control-plane adapters.
 * Handwritten Java APIs move with the repository; canonical wire compatibility is governed separately.</p>
 */
package org.pangrova.protocol.execution;
