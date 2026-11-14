package org.pangrova.connector.runtime.classloader;

import org.junit.jupiter.api.Test;

import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PluginClassLoader} delegation behaviour with SPI-first,
 * shared-first, and plugin-first strategies.
 */
class PluginClassLoaderTest {

    private static final URL[] EMPTY_URLS = new URL[0];

    @Test
    void domainApiAndSpiPackagesKeepHostIdentityWhenBundledByPlugin() throws Exception {
        var contracts = java.util.List.of(
                org.pangrova.indexing.api.descriptor.IndexDescriptor.class,
                org.pangrova.indexing.api.strategy.IndexStrategyProvider.class,
                org.pangrova.security.api.identity.UserIdentity.class,
                org.pangrova.security.api.authorization.AccessControl.class,
                org.pangrova.connector.api.plugin.ConnectorFactory.class,
                org.pangrova.connector.api.metadata.table.QualifiedTableName.class,
                org.pangrova.query.api.enums.QueryState.class,
                org.pangrova.query.api.lineage.QueryLineageFact.class,
                org.pangrova.governance.api.enums.ColumnDerivationKind.class,
                org.pangrova.query.api.audit.QueryAuditSink.class,
                org.pangrova.query.api.stage.StageStateListener.class,
                org.pangrova.ai.api.model.AiModelDescriptor.class,
                org.pangrova.ai.api.inference.InferenceProvider.class,
                org.pangrova.function.api.metadata.FunctionMetadata.class,
                org.pangrova.function.api.registry.FunctionFactory.class,
                org.pangrova.function.api.runtime.FunctionCallbackScope.class,
                org.pangrova.coordination.api.membership.WorkerSnapshot.class,
                org.pangrova.coordination.api.leadership.MaintenanceLeaderAware.class);
        URL[] urls = contracts.stream().map(type -> type.getProtectionDomain().getCodeSource().getLocation())
                .distinct().toArray(URL[]::new);
        try (var loader = new PluginClassLoader("bundled-domain-contracts", urls,
                getClass().getClassLoader(), java.util.Set.of())) {
            for (Class<?> contract : contracts) {
                assertThat(loader.loadClass(contract.getName())).isSameAs(contract);
            }
        }
    }

    @Test
    void onlyTheRequiredGovernanceEnumIsPinnedToTheHost() throws Exception {
        var shared = org.pangrova.governance.api.enums.ColumnDerivationKind.class;
        var privateType = org.pangrova.governance.api.enums.Severity.class;
        URL[] bundle = {shared.getProtectionDomain().getCodeSource().getLocation()};
        try (var loader = new PluginClassLoader("bundled-governance", bundle, getClass().getClassLoader(),
                java.util.Set.of())) {
            assertThat(loader.loadClass(shared.getName())).isSameAs(shared);
            assertThat(loader.loadClass(privateType.getName()).getClassLoader()).isSameAs(loader);
        }
    }

    @Test
    void engineTypesAndBatchContractsKeepHostIdentityEvenWhenBundledByPlugin() throws Exception {
        Class<?> engineType = org.pangrova.types.Type.class;
        Class<?> scalarValue = org.pangrova.types.value.ColumnValue.class;
        Class<?> batch = org.pangrova.batch.api.reader.ReadableBatch.class;
        URL[] pluginUrls = {engineType.getProtectionDomain().getCodeSource().getLocation(),
                batch.getProtectionDomain().getCodeSource().getLocation()};
        try (var loader = new PluginClassLoader("bundled-types", pluginUrls,
                getClass().getClassLoader(), java.util.Set.of())) {
            assertThat(loader.loadClass(engineType.getName())).isSameAs(engineType);
            assertThat(loader.loadClass(scalarValue.getName())).isSameAs(scalarValue);
            assertThat(loader.loadClass(batch.getName())).isSameAs(batch);
        }
    }

    @Test
    void spiClassesDelegateToSpiClassLoader() throws ClassNotFoundException {
        // org.pangrova.connector.api.plugin.Connector is an SPI class — must always
        // be loaded from the SPI classloader regardless of policy.
        PluginClassLoader cl = new PluginClassLoader(
                "test", EMPTY_URLS, getClass().getClassLoader());

        Class<?> loaded = cl.loadClass("org.pangrova.connector.api.plugin.Connector");
        assertThat(loaded).isSameAs(org.pangrova.connector.api.plugin.Connector.class);
    }

    @Test
    void sharedClassesDelegateToSpiClassLoaderFirst() throws ClassNotFoundException {
        // SLF4J is in the shared packages — should be resolvable from the
        // SPI classloader (which is our test classloader here).
        PluginClassLoader cl = new PluginClassLoader(
                "test-shared", EMPTY_URLS, getClass().getClassLoader());

        // org.slf4j.Logger is on the test classpath and in SHARED_PACKAGES
        Class<?> loaded = cl.loadClass("org.slf4j.Logger");
        assertThat(loaded).isNotNull();
    }

    @Test
    void pluginNameIsAccessible() {
        PluginClassLoader cl = new PluginClassLoader(
                "my-plugin", EMPTY_URLS, getClass().getClassLoader());

        assertThat(cl.getPluginName()).isEqualTo("my-plugin");
    }

    @Test
    void jdkClassesAreAlwaysAccessible() throws ClassNotFoundException {
        PluginClassLoader cl = new PluginClassLoader(
                "test", EMPTY_URLS, getClass().getClassLoader());

        // java.lang.String is always loadable (bootstrap classloader)
        Class<?> loaded = cl.loadClass("java.lang.String");
        assertThat(loaded).isSameAs(String.class);
    }

    @Test
    void sharesThePublicSignatureClosureAndKeepsUnrelatedImplementationsPluginLocal() throws Exception {
        var contracts = java.util.List.of(
                org.pangrova.common.exception.PangrovaException.class,
                org.pangrova.common.enums.PangrovaErrorCode.class,
                org.pangrova.common.file.CopyFileCommitBinding.class,
                org.pangrova.common.session.SessionContext.class,
                org.pangrova.execution.api.identity.QueryId.class,
                org.pangrova.observability.api.tracing.ExecutionTraceContext.class,
                org.pangrova.protocol.execution.AiTaskLifecycle.class,
                org.pangrova.protocol.v2.TaskRequest.Builder.class,
                com.google.protobuf.ByteString.class,
                org.pangrova.streaming.api.format.CdcFormatFactory.class,
                org.pangrova.streaming.api.source.SourceRecord.class);
        var privateTypes = java.util.List.of(
                org.pangrova.common.util.Preconditions.class,
                org.pangrova.protocol.rest.data.DataFile.class);
        URL[] bundle = java.util.stream.Stream.concat(contracts.stream(), privateTypes.stream())
                .map(type -> type.getProtectionDomain().getCodeSource().getLocation()).distinct().toArray(URL[]::new);
        try (var loader = new PluginClassLoader("bundled-contracts", bundle, getClass().getClassLoader(),
                java.util.Set.of())) {
            for (var type : contracts) assertThat(loader.loadClass(type.getName())).isSameAs(type);
            for (var type : privateTypes) assertThat(loader.loadClass(type.getName()).getClassLoader()).isSameAs(loader);
        }
    }
}
