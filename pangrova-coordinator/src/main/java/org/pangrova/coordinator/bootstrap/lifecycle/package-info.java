/**
 * Coordinator process lifecycle ownership. Owned by pangrova-coordinator; assembly and lifecycle
 * tests may consume its one-shot admission and actual dependency-exit barriers. It performs no SQL,
 * Native execution, transport routing, or durable execution-authority decisions.
 */
package org.pangrova.coordinator.bootstrap.lifecycle;
