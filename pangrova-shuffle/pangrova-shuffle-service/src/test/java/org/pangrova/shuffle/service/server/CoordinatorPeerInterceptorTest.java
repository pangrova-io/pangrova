package org.pangrova.shuffle.service.server;

import java.lang.reflect.Proxy;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Set;

import javax.net.ssl.SSLSession;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleRequest;
import org.pangrova.protocol.v2.shuffle.RegisterShuffleResponse;
import org.pangrova.protocol.v2.shuffle.RenewAccessRequest;
import org.pangrova.protocol.v2.shuffle.RenewAccessResponse;
import org.pangrova.protocol.v2.shuffle.ShuffleServiceGrpc;

import static org.assertj.core.api.Assertions.assertThat;

class CoordinatorPeerInterceptorTest {

    @Test
    void rejectsWorkerCertificateFromPrivilegedRegistration() throws Exception {
        CoordinatorPeerInterceptor interceptor = new CoordinatorPeerInterceptor(
                List.of("coordinator.internal"), ignored -> Set.of("worker.internal"));
        RecordingCall call = new RecordingCall(session());

        interceptor.interceptCall(call, new Metadata(), (ignored, headers) -> {
            throw new AssertionError("unauthorized Worker reached registration handler");
        });

        assertThat(call.status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    @Test
    void allowsExactCoordinatorSan() throws Exception {
        CoordinatorPeerInterceptor interceptor = new CoordinatorPeerInterceptor(
                List.of("coordinator.internal"), ignored -> Set.of("coordinator.internal"));

        assertThat(interceptor.isCoordinator(null)).isTrue();
    }

    @Test
    void marksCoordinatorRenewalButNeverElevatesWorkerRenewal() {
        CoordinatorPeerInterceptor coordinator = new CoordinatorPeerInterceptor(
                List.of("coordinator.internal"), ignored -> Set.of("coordinator.internal"));
        RenewalCall coordinatorCall = new RenewalCall(session());
        coordinator.interceptCall(coordinatorCall, new Metadata(), (ignored, headers) -> {
            assertThat(CoordinatorPeerInterceptor.isCoordinatorPeer()).isTrue();
            return new ServerCall.Listener<>() {};
        });
        assertThat(coordinatorCall.status).isNull();

        CoordinatorPeerInterceptor worker = new CoordinatorPeerInterceptor(
                List.of("coordinator.internal"), ignored -> Set.of("worker.internal"));
        RenewalCall workerCall = new RenewalCall(session());
        worker.interceptCall(workerCall, new Metadata(), (ignored, headers) -> {
            assertThat(CoordinatorPeerInterceptor.isCoordinatorPeer()).isFalse();
            return new ServerCall.Listener<>() {};
        });
        assertThat(workerCall.status).isNull();
    }

    @Test
    void rejectsRenewalWithoutMutualTls() {
        CoordinatorPeerInterceptor interceptor = new CoordinatorPeerInterceptor(
                List.of("coordinator.internal"), ignored -> Set.of());
        RenewalCall call = new RenewalCall(null);
        interceptor.interceptCall(call, new Metadata(), (ignored, headers) -> {
            throw new AssertionError("renewal without mTLS reached handler");
        });
        assertThat(call.status.getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    }

    private static SSLSession session() {
        return (SSLSession) Proxy.newProxyInstance(
                SSLSession.class.getClassLoader(), new Class<?>[] {SSLSession.class},
                (proxy, method, args) -> method.getName().equals("getPeerCertificates")
                        ? new Certificate[] {null} : defaultValue(method.getReturnType()));
    }

    private static Object defaultValue(Class<?> type) {
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        return null;
    }

    private static final class RecordingCall
            extends ServerCall<RegisterShuffleRequest, RegisterShuffleResponse> {
        private final Attributes attributes;
        private Status status;

        private RecordingCall(SSLSession session) {
            attributes = Attributes.newBuilder()
                    .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session).build();
        }

        @Override
        public void request(int messages) {}

        @Override
        public void sendHeaders(Metadata headers) {}

        @Override
        public void sendMessage(RegisterShuffleResponse message) {}

        @Override
        public void close(Status closeStatus, Metadata trailers) {
            status = closeStatus;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public MethodDescriptor<RegisterShuffleRequest, RegisterShuffleResponse>
                getMethodDescriptor() {
            return ShuffleServiceGrpc.getRegisterShuffleMethod();
        }

        @Override
        public Attributes getAttributes() {
            return attributes;
        }
    }

    private static final class RenewalCall
            extends ServerCall<RenewAccessRequest, RenewAccessResponse> {
        private final Attributes attributes;
        private Status status;

        private RenewalCall(SSLSession session) {
            attributes = session == null ? Attributes.EMPTY : Attributes.newBuilder()
                    .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session).build();
        }

        @Override
        public void request(int messages) {}

        @Override
        public void sendHeaders(Metadata headers) {}

        @Override
        public void sendMessage(RenewAccessResponse message) {}

        @Override
        public void close(Status closeStatus, Metadata trailers) {
            status = closeStatus;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public Attributes getAttributes() {
            return attributes;
        }

        @Override
        public MethodDescriptor<RenewAccessRequest, RenewAccessResponse> getMethodDescriptor() {
            return ShuffleServiceGrpc.getRenewAccessMethod();
        }
    }
}
