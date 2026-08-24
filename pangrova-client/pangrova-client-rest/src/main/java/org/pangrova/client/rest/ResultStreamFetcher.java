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
package org.pangrova.client.rest;

import org.pangrova.protocol.rest.query.ResultEndpoint;
import org.pangrova.transport.FlightResultFetcher;
import org.pangrova.transport.TransportConnectionException;
import org.pangrova.transport.TransportServerException;

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

    public ResultStreamFetcher(boolean useTls, String trustStorePath) {
        this.delegate = new FlightResultFetcher(useTls, trustStorePath);
    }

    /**
     * Fetches all result batches and returns them as a single Arrow IPC byte array.
     */
    public byte[] fetch(ResultEndpoint endpoint) {
        try {
            return delegate.fetch(endpoint);
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
