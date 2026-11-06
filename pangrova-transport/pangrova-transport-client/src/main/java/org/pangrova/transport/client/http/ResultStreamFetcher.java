/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.transport.client.http;

import org.pangrova.transport.api.result.ResultEndpoint;
import org.pangrova.transport.client.FlightResultFetcher;
import org.pangrova.transport.client.security.ResultTransportSecurity;
import org.pangrova.transport.client.TransportConnectionException;
import org.pangrova.transport.client.TransportServerException;
import org.pangrova.client.api.ResultBatchConsumer;
import org.pangrova.client.error.PangrovaConnectionException;
import org.pangrova.client.error.PangrovaServerException;

/**
 * Wraps {@link FlightResultFetcher} for result streaming and translates
 * transport-layer exceptions into {@link PangrovaClientException} variants.
 * Contains no Arrow imports — all Arrow/Flight code is confined to
 * {@code pangrova-transport}.
 */
public final class ResultStreamFetcher implements AutoCloseable {

    private final FlightResultFetcher delegate;

    public ResultStreamFetcher() {
        this.delegate = new FlightResultFetcher();
    }

    /**
     * Uses explicit result-channel policy, not a Java HTTP trust store.
     *
     * @param security immutable result policy and optional PEM material
     */
    public ResultStreamFetcher(ResultTransportSecurity security) {
        this.delegate = new FlightResultFetcher(security);
    }

    /**
     * Fetches all result batches and returns them as a single Arrow IPC byte array.
     */
    public byte[] fetch(ResultEndpoint endpoint) {
        return fetch(endpoint, Long.MAX_VALUE);
    }

    /** Fetches all batches while honoring an optional absolute query deadline. */
    public byte[] fetch(ResultEndpoint endpoint, long deadlineEpochMs) {
        try {
            return delegate.fetch(endpoint, deadlineEpochMs);
        } catch (TransportConnectionException e) {
            throw new PangrovaConnectionException(e.transportErrorCode(), e.getMessage(), e);
        } catch (TransportServerException e) {
            throw new PangrovaServerException(e.transportErrorCode(), e.getMessage(), e.statusCode(), e);
        }
    }

    /**
     * Streams result batches, invoking the consumer for each Arrow IPC batch.
     */
    public void fetchStream(ResultEndpoint endpoint, ResultBatchConsumer consumer) {
        try {
            delegate.fetchStream(endpoint, consumer::accept);
        } catch (TransportConnectionException e) {
            throw new PangrovaConnectionException(e.transportErrorCode(), e.getMessage(), e);
        } catch (TransportServerException e) {
            throw new PangrovaServerException(e.transportErrorCode(), e.getMessage(), e.statusCode(), e);
        }
    }

    @Override
    public void close() {
        delegate.close();
    }
}
