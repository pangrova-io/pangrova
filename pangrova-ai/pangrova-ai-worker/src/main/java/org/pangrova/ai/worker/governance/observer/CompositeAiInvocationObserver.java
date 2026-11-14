/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.pangrova.ai.worker.governance.observer;

import org.pangrova.ai.api.exception.AiInvocationException;
import org.pangrova.ai.api.invocation.AiInvocationRequest;
import org.pangrova.ai.api.invocation.AiInvocationResult;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Consumer;

/** Delivers terminal invocation events to every installed observer. */
public final class CompositeAiInvocationObserver implements AiInvocationObserver {

    private final List<AiInvocationObserver> observers;

    private CompositeAiInvocationObserver(List<AiInvocationObserver> observers) {
        this.observers = List.copyOf(observers);
    }

    /** Loads the installed observers and retains the canonical invocation logger. */
    public static AiInvocationObserver discover() {
        List<AiInvocationObserver> observers = new ArrayList<>();
        observers.add(new LoggingAiInvocationObserver());
        ServiceLoader.load(AiInvocationObserver.class).forEach(observers::add);
        return new CompositeAiInvocationObserver(observers);
    }

    @Override
    public void onSuccess(AiInvocationRequest request, AiInvocationResult result) {
        notifyObservers(observers, observer -> observer.onSuccess(request, result));
    }

    @Override
    public void onFailure(AiInvocationRequest request, AiInvocationException failure) {
        notifyObservers(observers, observer -> observer.onFailure(request, failure));
    }

    static void notifyObservers(
            List<AiInvocationObserver> observers, Consumer<AiInvocationObserver> notification) {
        RuntimeException firstFailure = null;
        for (AiInvocationObserver observer : observers) {
            try {
                notification.accept(observer);
            } catch (RuntimeException failure) {
                firstFailure = merge(firstFailure, failure);
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private static RuntimeException merge(RuntimeException firstFailure, RuntimeException failure) {
        if (firstFailure == null) {
            return failure;
        }
        firstFailure.addSuppressed(failure);
        return firstFailure;
    }
}
