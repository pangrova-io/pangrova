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
package org.pangrova.transport;

/**
 * Thrown when the Arrow Flight server returned an application-level error.
 * Carries an HTTP-equivalent status code for client-rest mapping.
 * Maps to an PangrovaServerException in the client-rest layer.
 */
public final class TransportServerException extends TransportException {

    private final int statusCode;

    public TransportServerException(String transportErrorCode, String message,
                                    int statusCode, Throwable cause) {
        super(transportErrorCode, message, cause);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }
}
