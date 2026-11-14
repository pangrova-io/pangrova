package org.pangrova.server.coordinator.http.endpoint;

import jakarta.annotation.Priority;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.ext.Provider;

/** Captures the transport peer before authentication; caller headers are not identity. */
@Provider
@PreMatching
@Priority(Priorities.AUTHENTICATION - 100)
public final class RequestPeerAddressFilter implements ContainerRequestFilter {
    static final String PEER_PROPERTY = "pangrova.http.transportPeer";
    @Context
    private HttpServletRequest servletRequest;

    @Override
    public void filter(ContainerRequestContext context) {
        if (servletRequest == null) throw new IllegalStateException("HTTP transport peer is not wired");
        String address = servletRequest.getRemoteAddr();
        if (address == null || address.isBlank()) throw new IllegalStateException("HTTP transport peer is missing");
        context.setProperty(PEER_PROPERTY, address);
    }

    static String resolve(ContainerRequestContext context) {
        Object address = context.getProperty(PEER_PROPERTY);
        return address instanceof String value && !value.isBlank() ? value : "unknown";
    }
}
