package org.pangrova.testing.e2e;

import org.pangrova.testing.e2e.ai.SharedAiProvider;
import org.pangrova.worker.bootstrap.NativeCapabilityAdvertisement;
import org.pangrova.worker.bootstrap.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap;
import org.pangrova.worker.jni.NativeFunctionConfig;
import org.pangrova.worker.jni.NativeRuntime;

/** One deployment, registration and publication owner for the shared E2E Native runtime. */
final class SharedNativeFunctionStartup implements AutoCloseable {
    private final SharedAiProvider provider = new SharedAiProvider();
    private final WorkerComponentRefs refs = new WorkerComponentRefs();
    private NativeFunctionConfig config;
    private NativeCapabilityAdvertisement.Advertisement advertisement;
    private boolean registrationEntered;

    NativeFunctionConfig prepare() {
        if (config == null) {
            try {
                config = provider.prepare();
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while preparing real AI models", failure);
            } catch (Exception failure) {
                throw new IllegalStateException("Failed to prepare real AI models", failure);
            }
        }
        return config;
    }

    NativeCapabilityAdvertisement.Advertisement initialize() {
        if (advertisement != null) {
            return advertisement;
        }
        if (config == null || !NativeRuntime.isInitialized()) {
            throw new IllegalStateException("Prepared models and the initialized Native runtime are required");
        }
        if (registrationEntered) {
            throw new IllegalStateException("The Native function startup requires cleanup, not re-registration");
        }
        registrationEntered = true;
        refs.setNativeAvailable(true);
        NativeFunctionBootstrap.initialize(refs, config);
        advertisement = NativeCapabilityAdvertisement.load(config);
        return advertisement;
    }

    SharedAiProvider provider() {
        if (advertisement == null) {
            throw new IllegalStateException("The shared Native function catalogue has not been published");
        }
        return provider;
    }

    @Override
    public void close() {
        if (NativeRuntime.isInitialized()) {
            throw new IllegalStateException("Native callback owners must exit before closing their provider");
        }
        NativeFunctionBootstrap.close(refs);
        provider.close();
    }
}
