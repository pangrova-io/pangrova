/**
 * Execution-plane seams between the Native function bridge and an optional
 * capability runtime.
 *
 * <p>The Worker dispatches Native callbacks without linking any capability
 * implementation. A runtime that backs provider functions installs these
 * ports through {@link java.util.ServiceLoader}. An absent runtime leaves the
 * bridge evaluating ordinary functions without binding any capability
 * authority and without projecting capability failures.</p>
 */
package org.pangrova.function.api.runtime;
