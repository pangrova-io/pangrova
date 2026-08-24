/**
 * SPI contracts for safe, zero-copy data transfer across the JNI boundary
 * between the Java control plane and the native (C++) execution engine.
 *
 * <p>This package addresses the memory safety gap identified in G-INTEROP-01:
 * Arrow's {@code release} callback on imported {@code ArrowArray} structs can
 * trigger C++ destructor logic on a Java thread, causing SIGSEGV. The solution
 * is a <b>native-handle ownership model</b> (following Gluten's design) where
 * the native side retains buffer ownership and Java holds only a ref-counted
 * ticket ({@link NativeBufferHandle}).</p>
 *
 * <p>Key types:</p>
 * <ul>
 *   <li>{@link NativeBufferHandle} -- ref-counted handle to native-owned
 *       buffer, safe for zero-copy JNI transfer</li>
 *   <li>{@link NativeBufferOwnership} -- enum of ownership models
 *       (COPIED, JAVA_RELEASE_CALLBACK, NATIVE_OWNED_HANDLE)</li>
 *   <li>{@link NativeBufferReleaser} -- callback for posting release
 *       requests to the native side</li>
 * </ul>
 *
 * @see org.pangrova.spi.execution.NativeColumnarStream
 * @see org.pangrova.spi.execution.InteropMode
 */
package org.pangrova.spi.execution.interop;
