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
 * Base exception for Arrow Flight transport failures.
 * Subclasses distinguish connection-level failures from server-reported errors.
 * Callers in client modules convert these to their own exception hierarchies
 * without importing any Arrow types.
 */
public class TransportException extends RuntimeException {

    private final String transportErrorCode;

    public TransportException(String transportErrorCode, String message, Throwable cause) {
        super(message, cause);
        this.transportErrorCode = transportErrorCode;
    }

    public String transportErrorCode() {
        return transportErrorCode;
    }
}
