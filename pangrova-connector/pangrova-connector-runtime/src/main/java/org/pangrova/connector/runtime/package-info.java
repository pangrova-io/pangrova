/**
 * Connector plugin runtime: discovery, classloader isolation, and lifecycle.
 *
 * <p>This module owns how the engine locates and isolates connector plugins.
 * The connector contracts themselves live in {@code org.pangrova.spi.connector}
 * so providers never depend on runtime code.</p>
 */
package org.pangrova.connector.runtime;
