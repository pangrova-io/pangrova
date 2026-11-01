package org.pangrova.shuffle.api.exchange;

/** Provider-neutral transport selected for one exchange input. */
public sealed interface ExchangeInputTransport
        permits DirectExchangeTransport, ExternalShuffleTransport {}
