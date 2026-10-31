/**
 * Worker-side AI execution plane: governed provider invocation, SQL AI functions, and the
 * ServiceLoader adapters that plug this runtime into the Worker bootstrap.
 *
 * <p>The Worker core never compiles against this module. Installing its JAR on the worker
 * classpath enables AI functions and AI model advertisement; leaving it out keeps the engine
 * running with non-AI SQL only.</p>
 */
package org.pangrova.ai.worker;
