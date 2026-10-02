package org.pangrova.shuffle.deployment;

import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import io.grpc.Grpc;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;

/** Restricts privileged lifecycle RPCs to configured Coordinator certificate SANs. */
public final class CoordinatorPeerInterceptor implements ServerInterceptor {

    private static final int DNS_SAN = 2;
    private static final int URI_SAN = 6;
    private static final Context.Key<Boolean> COORDINATOR_PEER =
            Context.key("shuffle-coordinator-peer");

    private final Set<String> coordinatorIdentities;
    private final Set<String> privilegedMethods;
    private final CertificateIdentityExtractor identityExtractor;

    /** Creates an interceptor with exact DNS/URI SAN identities. */
    public CoordinatorPeerInterceptor(List<String> coordinatorIdentities) {
        this(coordinatorIdentities, CoordinatorPeerInterceptor::peerIdentities);
    }

    CoordinatorPeerInterceptor(
            List<String> coordinatorIdentities,
            CertificateIdentityExtractor identityExtractor) {
        this.coordinatorIdentities = Set.copyOf(coordinatorIdentities);
        this.identityExtractor = identityExtractor;
        this.privilegedMethods = Set.of(
                ShuffleServiceGrpc.getRegisterShuffleMethod().getFullMethodName(),
                ShuffleServiceGrpc.getFinalizeShuffleMethod().getFullMethodName(),
                ShuffleServiceGrpc.getCleanupMethod().getFullMethodName());
    }

    @Override
    public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
            ServerCall<RequestT, ResponseT> call,
            Metadata headers,
            ServerCallHandler<RequestT, ResponseT> next) {
        String method = call.getMethodDescriptor().getFullMethodName();
        boolean renewal = method.equals(
                ShuffleServiceGrpc.getRenewAccessMethod().getFullMethodName());
        if (!renewal && !privilegedMethods.contains(method)) {
            return next.startCall(call, headers);
        }
        SSLSession session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
        if (session == null) {
            return close(call, Status.UNAUTHENTICATED.withDescription(
                    "privileged shuffle RPC requires mTLS"));
        }
        try {
            X509Certificate certificate = (X509Certificate) session.getPeerCertificates()[0];
            boolean coordinator = isCoordinator(certificate);
            if (renewal) {
                return Contexts.interceptCall(
                        Context.current().withValue(COORDINATOR_PEER, coordinator),
                        call, headers, next);
            }
            if (!coordinator) {
                return close(call, Status.PERMISSION_DENIED.withDescription(
                        "client certificate is not an authorized Coordinator identity"));
            }
            return next.startCall(call, headers);
        } catch (SSLPeerUnverifiedException | CertificateParsingException failure) {
            return close(call, Status.UNAUTHENTICATED.withCause(failure)
                    .withDescription("client certificate identity cannot be verified"));
        }
    }

    boolean isCoordinator(X509Certificate certificate) throws CertificateParsingException {
        return identityExtractor.extract(certificate).stream()
                .anyMatch(coordinatorIdentities::contains);
    }

    /**
     * Whether the current mTLS peer is an authorized Coordinator identity.
     *
     * @return {@code true} only for a verified Coordinator certificate SAN
     */
    public static boolean isCoordinatorPeer() {
        return Boolean.TRUE.equals(COORDINATOR_PEER.get());
    }

    private static Set<String> peerIdentities(X509Certificate certificate)
            throws CertificateParsingException {
        Collection<List<?>> names = certificate.getSubjectAlternativeNames();
        Set<String> identities = new HashSet<>();
        if (names == null) {
            return identities;
        }
        for (List<?> name : names) {
            int type = (Integer) name.get(0);
            if ((type == DNS_SAN || type == URI_SAN) && name.get(1) instanceof String identity) {
                identities.add(identity);
            }
        }
        return identities;
    }

    private static <RequestT, ResponseT> ServerCall.Listener<RequestT> close(
            ServerCall<RequestT, ResponseT> call,
            Status status) {
        call.close(status, new Metadata());
        return new ServerCall.Listener<>() {};
    }

    @FunctionalInterface
    interface CertificateIdentityExtractor {
        Set<String> extract(X509Certificate certificate) throws CertificateParsingException;
    }
}
