/**
 * Worker startup resource ownership and retryable cleanup ledgers.
 *
 * <p>Owned by pangrova-worker-core; internal and not a stable extension API.
 * Worker assembly, startup phases and lifecycle checks may consume this package.
 * It must not own phase execution, deployment configuration or coordinator authority.
 */
package org.pangrova.worker.bootstrap.ownership;
