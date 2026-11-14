package org.pangrova.shuffle.exchange;

/** Provider-neutral transport selected for one exchange input. */
public sealed interface ExchangeInputTransport
        permits DirectExchangeTransport, ExternalShuffleTransport {}
