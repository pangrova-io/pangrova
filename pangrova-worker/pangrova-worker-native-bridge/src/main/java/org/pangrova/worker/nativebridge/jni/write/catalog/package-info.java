/**
 * Worker-owned exact catalog deployment, connector revision leases and local storage binding.
 * Internal to Worker bootstrap, task creation and dataset control; no row execution or JNI entry points.
 * These owners retain failed connector shutdowns and never publish Coordinator catalog mutations.
 */
package org.pangrova.worker.nativebridge.jni.write.catalog;
