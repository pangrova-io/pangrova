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
    private final SharedGovernedFileStorage files = new SharedGovernedFileStorage();
    private final WorkerComponentRefs refs = new WorkerComponentRefs();
    private NativeFunctionConfig config;
    private NativeCapabilityAdvertisement.Advertisement advertisement;
    private boolean registrationEntered;

    NativeFunctionConfig prepare() {
        if (config == null) {
            try {
                var prepared = provider.prepare();
                var fileConfiguration = files.prepareConfiguredDeployment();
                if (fileConfiguration.isEmpty()) {
                    config = prepared;
                } else {
                    var plugins = new java.util.LinkedHashMap<>(prepared.pluginConfigs());
                    var ai = new java.util.LinkedHashMap<>(prepared.configForPlugin("ai-inference"));
                    for (var entry : fileConfiguration.entrySet()) {
                        if (ai.putIfAbsent(entry.getKey(), entry.getValue()) != null) {
                            throw new IllegalStateException(
                                    "Duplicate production FILE configuration: " + entry.getKey());
                        }
                    }
                    plugins.put("ai-inference", java.util.Map.copyOf(ai));
                    config = new NativeFunctionConfig(prepared.autoRegister(), plugins);
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                closeAfterPreparationFailure(failure);
                throw new IllegalStateException("Interrupted while preparing real AI/FILE deployments", failure);
            } catch (Exception failure) {
                closeAfterPreparationFailure(failure);
                throw new IllegalStateException("Failed to prepare real AI/FILE deployments", failure);
            } catch (Error failure) {
                closeAfterPreparationFailure(failure);
                throw failure;
            }
        }
        return config;
    }

    private void closeAfterPreparationFailure(Throwable failure) {
        try {
            close();
        } catch (RuntimeException cleanup) {
            failure.addSuppressed(cleanup);
        }
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

    SharedGovernedFileStorage fileStorage() {
        if (advertisement == null) {
            throw new IllegalStateException("The shared Native function catalogue has not been published");
        }
        return files;
    }

    @Override
    public void close() {
        if (NativeRuntime.isInitialized()) {
            throw new IllegalStateException("Native callback owners must exit before closing their provider");
        }
        Exception failure = null;
        for (AutoCloseable owner : new AutoCloseable[]{() -> NativeFunctionBootstrap.close(refs), provider, files}) {
            try { owner.close(); } catch (Exception cleanup) {
                if (failure == null) {
                    failure = cleanup;
                } else {
                    failure.addSuppressed(cleanup);
                }
            }
        }
        if (failure != null) throw new IllegalStateException("Shared function deployment cleanup failed", failure);
    }
}
