package org.pangrova.shuffle.exchange;

import java.util.List;

/** Direct Worker-to-Worker exchange sources. */
public record DirectExchangeTransport(List<ExchangeEndpoint> sources)
        implements ExchangeInputTransport {

    public DirectExchangeTransport {
        sources = List.copyOf(sources == null ? List.of() : sources);
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("direct exchange sources must not be empty");
        }
    }
}
