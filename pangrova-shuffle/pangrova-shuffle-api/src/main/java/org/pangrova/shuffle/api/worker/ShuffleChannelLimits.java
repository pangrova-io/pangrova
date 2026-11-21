package org.pangrova.shuffle.api.worker;

/** Maximum simultaneously creating or owned channels across all readers and producers of one Worker. */
public record ShuffleChannelLimits(int maxOwnedChannels, int maxOwnedChannelsPerEndpoint) {
    public ShuffleChannelLimits {
        if (maxOwnedChannels <= 0 || maxOwnedChannelsPerEndpoint <= 0
                || maxOwnedChannelsPerEndpoint > maxOwnedChannels) {
            throw new IllegalArgumentException("Channel limits require 0 < per-endpoint <= total");
        }
    }
}
