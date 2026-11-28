package org.pangrova.governance.runtime.quality;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.common.concurrency.ThreadSafe;
import org.pangrova.governance.api.quality.AlertChannel;
import org.pangrova.governance.api.enums.Severity;

/**
 * Registry for alert delivery channels.
 *
 * <p>Concurrency strategy: CopyOnWriteArrayList for channel list.</p>
 */
@ThreadSafe
public class AlertChannelRegistry {

    private final List<AlertChannel> channels = new CopyOnWriteArrayList<>();

    public void register(AlertChannel channel) {
        channels.add(channel);
    }

    public List<AlertChannel> getChannels(Severity severity) {
        return channels.stream()
            .filter(ch -> ch.supports(severity))
            .toList();
    }
}
