/**
 * Worker readiness, draining and dependency-ordered resource shutdown.
 *
 * <p>Owned by pangrova-worker-core; internal and not a stable extension API.
 * Worker assembly and startup phases may consume this package.
 * It must not own startup orchestration or deployment configuration.
 */
package org.pangrova.worker.bootstrap.lifecycle;
