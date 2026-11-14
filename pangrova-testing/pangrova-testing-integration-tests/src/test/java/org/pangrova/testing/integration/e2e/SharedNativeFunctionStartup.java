package org.pangrova.testing.integration.e2e;

import org.pangrova.testing.integration.e2e.ai.SharedAiProvider;
import org.pangrova.testing.integration.e2e.file.FileStorageDeployment;
import org.pangrova.testing.integration.e2e.file.LocalFileDeployment;
import org.pangrova.testing.integration.e2e.file.S3FileDeployment;
import org.pangrova.worker.bootstrap.NativeCapabilityAdvertisement;
import org.pangrova.worker.bootstrap.ownership.WorkerComponentRefs;
import org.pangrova.worker.bootstrap.phase.NativeFunctionBootstrap;
import org.pangrova.worker.function.NativeFunctionConfig;
import org.pangrova.worker.jni.NativeRuntime;

/** One deployment, registration and publication owner for the shared E2E Native runtime. */
final class SharedNativeFunctionStartup implements AutoCloseable {
    private SharedAiProvider provider;
    private final FileStorageDeployment files = FileStorageDeployment.configured();
    private final WorkerComponentRefs refs = new WorkerComponentRefs();
    private NativeFunctionConfig config;
    private NativeCapabilityAdvertisement.Advertisement advertisement;
    private boolean registrationEntered;

    NativeFunctionConfig prepare() {
        if (config == null) {
            try {
                if (provider == null) {
                    provider = new SharedAiProvider();
                }
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

    S3FileDeployment s3FileStorage() {
        requireFileAdvertisement();
        if (files instanceof S3FileDeployment s3) return s3;
        throw new IllegalStateException("The shared FILE deployment is not S3");
    }

    LocalFileDeployment localFileStorage() {
        requireFileAdvertisement();
        if (files instanceof LocalFileDeployment local) return local;
        throw new IllegalStateException("The shared FILE deployment is not LOCAL");
    }

    private void requireFileAdvertisement() {
        if (advertisement == null) {
            throw new IllegalStateException("The shared Native function catalogue has not been published");
        }
    }

    @Override
    public void close() {
        if (NativeRuntime.isInitialized()) {
            throw new IllegalStateException("Native callback owners must exit before closing their provider");
        }
        Exception failure = null;
        for (AutoCloseable owner : new AutoCloseable[]{() -> NativeFunctionBootstrap.close(refs), provider, files}) {
            if (owner == null) continue;
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
