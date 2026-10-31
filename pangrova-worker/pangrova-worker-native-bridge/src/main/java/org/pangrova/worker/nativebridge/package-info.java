// Copyright (c) Pangrova contributors. All rights reserved.
// Licensed under the Apache License, Version 2.0.

/**
 * Native bridge: JNI entry points, library loading, callback contracts, and
 * native write-path adapters shared by the Worker and AI worker artifacts.
 *
 * <p>This module owns every {@code Java_org_pangrova_*} symbol exported by the
 * native library. Host artifacts assemble behavior by installing
 * {@link org.pangrova.function.api.runtime.FunctionCallbackScope} and
 * {@link org.pangrova.function.api.runtime.FunctionFailureProjector}
 * providers; the bridge itself has no dependency on Worker or AI packages.</p>
 */
package org.pangrova.worker.nativebridge;
