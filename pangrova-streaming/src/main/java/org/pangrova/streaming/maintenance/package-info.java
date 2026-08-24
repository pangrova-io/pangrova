/**
 * Table maintenance scheduling and execution: compaction, snapshot expiration,
 * orphan cleanup.
 *
 * <p>Sub-packages:</p>
 * <ul>
 *   <li>{@code maintenance/policy/} — RetentionPolicy, RetentionPolicyManager,
 *       RetentionAction, TriggerPriority, CompactionPolicy, SnapshotExpirationPolicy</li>
 * </ul>
 *
 * <p>Parent package: scheduling core (MaintenanceScheduler, MaintenanceTask,
 * MaintenanceConfig, MaintenanceStatus, TableMaintenanceSpi) and result types
 * (CompactionResult, SnapshotExpirationResult, OrphanCleanupResult).</p>
 */
package org.pangrova.streaming.maintenance;
