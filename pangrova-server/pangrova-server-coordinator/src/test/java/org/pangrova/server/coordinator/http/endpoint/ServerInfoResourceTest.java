package org.pangrova.server.coordinator.http.endpoint;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Response;
import org.pangrova.common.client.constants.ApiVersion;
import org.pangrova.protocol.rest.version.ServerInfo;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ServerInfoResource}.
 * Verifies the /v1/server/info endpoint returns correct version metadata.
 */
class ServerInfoResourceTest {

    @Test
    void returnsServerInfoWithVersion() {
        ServerInfoResource resource = new ServerInfoResource("0.1.0-SNAPSHOT");

        Response response = resource.getServerInfo();

        assertThat(response.getStatus()).isEqualTo(200);
        ServerInfo info = (ServerInfo) response.getEntity();
        assertThat(info.serverVersion()).isEqualTo("0.1.0-SNAPSHOT");
        assertThat(info.apiVersion()).isEqualTo(ApiVersion.CURRENT);
        assertThat(info.minApiVersion()).isEqualTo(ApiVersion.MIN_SUPPORTED);
        assertThat(info.serverName()).isEqualTo("Pangrova");
    }

    @Test
    void acceptsPrebuiltServerInfo() {
        ServerInfo custom = new ServerInfo("1.2.3", 5, 3, "CustomPangrova");
        ServerInfoResource resource = new ServerInfoResource(custom);

        Response response = resource.getServerInfo();

        assertThat(response.getStatus()).isEqualTo(200);
        ServerInfo info = (ServerInfo) response.getEntity();
        assertThat(info.serverVersion()).isEqualTo("1.2.3");
        assertThat(info.apiVersion()).isEqualTo(5);
        assertThat(info.minApiVersion()).isEqualTo(3);
        assertThat(info.serverName()).isEqualTo("CustomPangrova");
    }
}
