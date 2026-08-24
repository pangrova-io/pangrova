/**
 * TODO: Speculative execution removed (was non-functional no-op with write double-commit risk).
 * Re-implement with proper read-only task gating + winner/loser cancel protocol.
 *
 * <p>The previous implementation (SpeculativeTaskLauncher, SpeculativeStageHandler,
 * SpeculativeExecutionScheduler, StragglerDetector, DuplicateTaskResolver, SpeculationConfig)
 * only generated "spec-" task IDs and stored them in maps but NEVER actually cloned the
 * plan/splits or dispatched a real duplicate task to a worker. It was a complete no-op for
 * straggler mitigation. Additionally, there was no winner/loser resolution that would prevent
 * double-commit for write tasks.</p>
 *
 * <p>Removed as part of finding F-COORD-SCHED-07.</p>
 *
 * @see <a href="https://github.com/pangrova/docs/architecture">Architecture Docs</a>
 */
package org.pangrova.coordinator.speculation;
