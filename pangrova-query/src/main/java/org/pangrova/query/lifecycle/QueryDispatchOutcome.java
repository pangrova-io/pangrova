package org.pangrova.query.lifecycle;

import org.pangrova.common.client.result.ResultEndpoint;

/** Closed control-plane outcome of one query dispatch. */
public record QueryDispatchOutcome(ResultEndpoint resultEndpoint) {}
