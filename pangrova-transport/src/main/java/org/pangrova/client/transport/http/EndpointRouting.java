package org.pangrova.client.transport.http;

import org.pangrova.common.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/** Owns bounded endpoint selection and identity-based failure advancement for one REST client. */
@ThreadSafe
final class EndpointRouting {
    private static final Logger LOG = LoggerFactory.getLogger(EndpointRouting.class);
    private final List<String> endpoints;
    private final String scheme;
    private final AtomicReference<Selection> preferred;

    EndpointRouting(List<String> endpoints, String scheme) {
        this.endpoints = List.copyOf(endpoints);
        if (this.endpoints.isEmpty()) {
            throw new IllegalArgumentException("Endpoint routing requires at least one endpoint");
        }
        this.scheme = scheme;
        preferred = new AtomicReference<>(new Selection(0));
    }

    Selection current() {
        return preferred.get();
    }

    SelectedRequest bind(Selection selection, HttpRequest request) {
        requireOwned(selection);
        URI endpoint = URI.create(selection.baseUrl());
        if (!endpoint.getScheme().equals(request.uri().getScheme())
                || !endpoint.getRawAuthority().equals(request.uri().getRawAuthority())) {
            throw new IllegalArgumentException("HTTP request does not address its captured endpoint selection");
        }
        return new SelectedRequest(selection, request);
    }

    void validate(SelectedRequest request) {
        requireOwned(request.selection());
    }

    Selection failed(Selection failed) {
        requireOwned(failed);
        if (endpoints.size() == 1) {
            return failed;
        }
        var next = new Selection(failed.index == endpoints.size() - 1 ? 0 : failed.index + 1);
        // The reference itself is the generation. Revisiting an index creates a
        // different generation, so an old failure cannot cause an ABA rollback.
        if (preferred.compareAndSet(failed, next)) {
            LOG.info("event=endpoint_failover newEndpoint={}", endpoints.get(next.index));
        }
        return next;
    }

    private void requireOwned(Selection selection) {
        if (selection.owner() != this) {
            throw new IllegalArgumentException("Endpoint selection belongs to another REST client");
        }
    }

    final class Selection {
        private final int index;

        private Selection(int index) {
            this.index = index;
        }

        int index() {
            return index;
        }

        String baseUrl() {
            return scheme + "://" + endpoints.get(index);
        }

        private EndpointRouting owner() {
            return EndpointRouting.this;
        }
    }

    static final class SelectedRequest {
        private final Selection selection;
        private final HttpRequest request;

        private SelectedRequest(Selection selection, HttpRequest request) {
            this.selection = selection;
            this.request = request;
        }

        Selection selection() {
            return selection;
        }

        HttpRequest request() {
            return request;
        }
    }
}
