/**
 * Canonical task lifecycle contracts owned by pangrova-protocol.
 *
 * <p>Coordinator, Worker, connector and transport consumers share these admission,
 * cancellation-seal and terminal-report authorities. Helpers preserve the v2 wire contract
 * and validate evidence; they must not execute tasks, retain runtime owners or perform I/O.
 */
package org.pangrova.protocol.task;
