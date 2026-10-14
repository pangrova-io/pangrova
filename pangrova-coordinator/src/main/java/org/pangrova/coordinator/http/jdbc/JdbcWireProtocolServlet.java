package org.pangrova.coordinator.http.jdbc;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.Handler.HandlerResponse;
import org.apache.calcite.avatica.remote.JsonHandler;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.ProtobufHandler;
import org.apache.calcite.avatica.remote.ProtobufTranslation;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.Service;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Servlet that exposes the Avatica JDBC wire protocol over HTTP.
 * Supports both JSON and Protobuf serialization formats, auto-detected
 * from the request Content-Type header.
 *
 * <p>Mounted at {@code /jdbc} on the coordinator HTTP server, enabling
 * standard JDBC tools (DBeaver, IntelliJ Database Tools, Tableau) to
 * connect without a custom driver.</p>
 */
public final class JdbcWireProtocolServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcWireProtocolServlet.class);
    private static final String CONTENT_TYPE_PROTOBUF = "application/x-google-protobuf";

    private final transient JsonHandler jsonHandler;
    private final transient ProtobufHandler protobufHandler;

    /** Builds the JSON and Protobuf wire handlers over the given Avatica metadata. */
    public JdbcWireProtocolServlet(Meta meta) {
        Service service = new LocalService(meta);
        this.jsonHandler = new JsonHandler(service, NoopMetricsSystem.getInstance());
        ProtobufTranslation translation = new ProtobufTranslationImpl();
        this.protobufHandler = new ProtobufHandler(service, translation, NoopMetricsSystem.getInstance());
        LOG.info("event=jdbc_wire_protocol_initialized");
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String contentType = req.getContentType();
        byte[] requestBody = req.getInputStream().readAllBytes();

        if (contentType != null && contentType.contains(CONTENT_TYPE_PROTOBUF)) {
            HandlerResponse<byte[]> handlerResp = protobufHandler.apply(requestBody);
            resp.setStatus(handlerResp.getStatusCode());
            resp.setContentType(CONTENT_TYPE_PROTOBUF);
            resp.getOutputStream().write(handlerResp.getResponse());
        } else {
            String requestJson = new String(requestBody, StandardCharsets.UTF_8);
            HandlerResponse<String> handlerResp = jsonHandler.apply(requestJson);
            resp.setStatus(handlerResp.getStatusCode());
            resp.setContentType("application/json;charset=utf-8");
            resp.getWriter().write(handlerResp.getResponse());
        }
    }
}
