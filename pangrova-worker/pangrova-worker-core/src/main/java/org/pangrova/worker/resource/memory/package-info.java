/**
 * Worker memory budgets, pool accounting and process-lifetime shuffle reservations.
 *
 * <p>Owned by pangrova-worker-core; internal and not a stable extension API.
 * Worker resources, tasks and bootstrap adapters may consume this package.
 * It must not own startup orchestration or coordinator state.
 */
package org.pangrova.worker.resource.memory;
