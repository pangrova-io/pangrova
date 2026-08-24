package org.pangrova.governance.quality.alert;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.pangrova.common.concurrent.ThreadSafe;

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
