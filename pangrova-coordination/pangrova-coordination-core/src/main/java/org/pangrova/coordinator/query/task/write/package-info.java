/**
 * Coordinator-owned write allocation, fragment tracking and fenced commit-recovery contracts.
 * Scheduling and report completion consume these owners; recovery invokes an explicit reconciler
 * through the existing catalog adapter. This leaf depends on SPI coordination/connector values,
 * not task-report reception, query scheduling, Native execution or another wire protocol.
 */
package org.pangrova.coordinator.query.task.write;
