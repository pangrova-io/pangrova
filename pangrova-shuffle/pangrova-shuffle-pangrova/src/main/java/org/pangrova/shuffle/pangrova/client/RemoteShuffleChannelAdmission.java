package org.pangrova.shuffle.pangrova.client;

import org.pangrova.shuffle.nativewire.exception.ShuffleChannelAdmissionException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import io.grpc.ManagedChannel;
import org.pangrova.common.concurrent.GuardedBy;
import org.pangrova.common.concurrent.ThreadSafe;

/** Worker-wide credits for owned ManagedChannels, including creation and failed joins. */
@ThreadSafe
public final class RemoteShuffleChannelAdmission {
    private static final int MAX_TCP_PORT = 65_535;

    /** An exact TLS-bound address, not a DNS/IP equivalence or an OS socket identity. */
    public record Endpoint(String host, int port, String tlsIdentity) {
        public Endpoint {
            if (host == null
                    || host.isBlank()
                    || port <= 0
                    || port > MAX_TCP_PORT
                    || tlsIdentity == null) {
                throw new IllegalArgumentException(
                        "Channel endpoint requires host, port and TLS identity");
            }
            host = host.toLowerCase(Locale.ROOT);
        }
    }

    public record Snapshot(
            int ownedAndCreatingChannels,
            int endpointCount,
            Map<Endpoint, Integer> channelsByEndpoint) {
        /** Captures actual credits without exposing a mutable caller-owned map. */
        public Snapshot {
            channelsByEndpoint = Map.copyOf(channelsByEndpoint);
        }
    }

    private final org.pangrova.shuffle.api.worker.ShuffleChannelLimits limits;

    @GuardedBy("this")
    private final Set<Reservation> reservations = new HashSet<>();

    @GuardedBy("this")
    private final Map<Endpoint, EndpointCredits> counts = new HashMap<>();

    @GuardedBy("this")
    private boolean sealed;

    public RemoteShuffleChannelAdmission(org.pangrova.shuffle.api.worker.ShuffleChannelLimits limits) {
        this.limits = java.util.Objects.requireNonNull(limits, "limits");
    }

    synchronized Reservation reserve(Endpoint endpoint) {
        requireOpen();
        EndpointCredits previous = counts.get(endpoint);
        int endpointCount = previous == null ? 0 : previous.count;
        if (reservations.size() >= limits.maxOwnedChannels()) {
            throw new ShuffleChannelAdmissionException(
                    "TOTAL", reservations.size(), limits.maxOwnedChannels());
        }
        if (endpointCount >= limits.maxOwnedChannelsPerEndpoint()) {
            throw new ShuffleChannelAdmissionException(
                    "ENDPOINT", endpointCount, limits.maxOwnedChannelsPerEndpoint());
        }
        var reservation = new Reservation(this, endpoint);
        var credits = previous == null ? new EndpointCredits() : previous;
        try {
            if (previous == null) {
                counts.put(endpoint, credits);
            }
            reservations.add(reservation);
        } catch (RuntimeException | Error failure) {
            reservations.remove(reservation);
            if (previous == null) {
                counts.remove(endpoint, credits);
            }
            throw failure;
        }
        credits.count++;
        return reservation;
    }

    public synchronized Snapshot snapshot() {
        var values = new HashMap<Endpoint, Integer>();
        counts.forEach((endpoint, credits) -> values.put(endpoint, credits.count));
        return new Snapshot(reservations.size(), counts.size(), Map.copyOf(values));
    }

    public synchronized void seal() {
        sealed = true;
    }

    synchronized void requireOpen() {
        if (sealed) {
            throw new IllegalStateException("Worker shuffle channel admission is sealed");
        }
    }

    /** Requires every real channel construction and termination owner to have exited. */
    public synchronized void requireDrained() {
        if (!reservations.isEmpty()) {
            throw new IllegalStateException(
                    "Worker still owns shuffle channel reservations: " + reservations.size());
        }
    }

    private synchronized void release(Reservation reservation) {
        if (!reservation.constructionExited
                || reservation.channel != null && !reservation.channel.isTerminated()) {
            throw new IllegalStateException(
                    "Channel credit requires actual construction exit and termination");
        }
        if (reservations.contains(reservation)) {
            // Returning an actual terminated owner must not allocate a boxed
            // counter while already handling an allocation failure.
            var credits = counts.get(reservation.endpoint);
            if (--credits.count == 0) {
                counts.remove(reservation.endpoint);
            }
            reservations.remove(reservation);
        }
    }

    /** Accessed exclusively under the owning admission monitor, like the counts map. */
    private static final class EndpointCredits {
        private int count;
    }

    /** A creating reservation becomes an owner without releasing/reacquiring its credit. */
    @ThreadSafe
    static final class Reservation {
        private final RemoteShuffleChannelAdmission admission;
        private final Endpoint endpoint;
        private volatile ManagedChannel channel;
        private volatile boolean constructionExited;

        Reservation(RemoteShuffleChannelAdmission admission, Endpoint endpoint) {
            this.admission = admission;
            this.endpoint = endpoint;
        }

        void attach(ManagedChannel actualChannel) {
            if (this.channel != null || constructionExited) {
                throw new IllegalStateException("Channel already bound");
            }
            this.channel = java.util.Objects.requireNonNull(actualChannel, "channel");
        }

        void constructionExited() {
            constructionExited = true;
        }

        boolean hasConstructionExited() {
            return constructionExited;
        }

        ManagedChannel channel() {
            return channel;
        }

        void releaseTerminated() {
            admission.release(this);
        }
    }
}
