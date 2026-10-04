/**
 * Canonical protocol primitives owned by pangrova-protocol.
 *
 * <p>Engine, connector and client modules may consume these shared wire authorities. Task
 * admission, terminal delivery and execution-drain helpers belong to the task subpackage;
 * this package must not implement scheduling, execution or persistent ownership.
 */
package org.pangrova.protocol;
