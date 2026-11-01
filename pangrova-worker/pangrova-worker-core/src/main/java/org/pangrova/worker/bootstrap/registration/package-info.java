/**
 * Worker registration HTTP transport.
 *
 * <p>Owned by pangrova-worker-core; internal and not a stable extension API.
 * Worker assembly, heartbeat registration and shutdown may consume this package.
 * It must not own resource aggregation, task execution or startup ordering.
 */
package org.pangrova.worker.bootstrap.registration;
