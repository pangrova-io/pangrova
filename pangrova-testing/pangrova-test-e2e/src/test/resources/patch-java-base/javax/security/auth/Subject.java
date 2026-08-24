package javax.security.auth;

import java.io.Serializable;
import java.security.AccessControlContext;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Patched Subject class for E2E tests.
 * Provides getSubject() stub (needed by Hadoop UGI) on JDK 17+
 * where AccessControlContext-based Subject retrieval is deprecated/removed.
 * Compiled for JDK 17 bytecode compatibility.
 */
public final class Subject implements Serializable {

    private static final long serialVersionUID = -8308522755600156056L;

    transient Set<Principal> principals;
    transient Set<Object> pubCredentials;
    transient Set<Object> privCredentials;
    private volatile boolean readOnly;

    private static final ThreadLocal<Subject> CURRENT_SUBJECT = new ThreadLocal<>();

    public Subject() {
        this.principals = Collections.synchronizedSet(new HashSet<>());
        this.pubCredentials = Collections.synchronizedSet(new HashSet<>());
        this.privCredentials = Collections.synchronizedSet(new HashSet<>());
    }

    public Subject(boolean readOnly, Set<? extends Principal> principals,
                   Set<?> pubCredentials, Set<?> privCredentials) {
        this.principals = Collections.synchronizedSet(new HashSet<>(principals));
        this.pubCredentials = Collections.synchronizedSet(new HashSet<>(pubCredentials));
        this.privCredentials = Collections.synchronizedSet(new HashSet<>(privCredentials));
        this.readOnly = readOnly;
    }

    public void setReadOnly() {
        this.readOnly = true;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @SuppressWarnings("removal")
    public static Subject getSubject(AccessControlContext acc) {
        return CURRENT_SUBJECT.get();
    }

    public static Subject current() {
        return CURRENT_SUBJECT.get();
    }

    public static <T> T callAs(Subject subject, Callable<T> action) throws Exception {
        Subject prev = CURRENT_SUBJECT.get();
        CURRENT_SUBJECT.set(subject);
        try {
            return action.call();
        } finally {
            CURRENT_SUBJECT.set(prev);
        }
    }

    @SuppressWarnings("removal")
    public static <T> T doAs(Subject subject, PrivilegedAction<T> action) {
        Subject prev = CURRENT_SUBJECT.get();
        CURRENT_SUBJECT.set(subject);
        try {
            return action.run();
        } finally {
            CURRENT_SUBJECT.set(prev);
        }
    }

    @SuppressWarnings("removal")
    public static <T> T doAs(Subject subject, PrivilegedExceptionAction<T> action)
            throws PrivilegedActionException {
        Subject prev = CURRENT_SUBJECT.get();
        CURRENT_SUBJECT.set(subject);
        try {
            return action.run();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new PrivilegedActionException(e);
        } finally {
            CURRENT_SUBJECT.set(prev);
        }
    }

    @SuppressWarnings("removal")
    public static <T> T doAsPrivileged(Subject subject, PrivilegedAction<T> action,
                                        AccessControlContext acc) {
        return doAs(subject, action);
    }

    @SuppressWarnings("removal")
    public static <T> T doAsPrivileged(Subject subject, PrivilegedExceptionAction<T> action,
                                        AccessControlContext acc)
            throws PrivilegedActionException {
        return doAs(subject, action);
    }

    public Set<Principal> getPrincipals() {
        return principals;
    }

    @SuppressWarnings("unchecked")
    public <T extends Principal> Set<T> getPrincipals(Class<T> c) {
        Set<T> result = new HashSet<>();
        for (Principal p : principals) {
            if (c.isInstance(p)) {
                result.add((T) p);
            }
        }
        return result;
    }

    public Set<Object> getPublicCredentials() {
        return pubCredentials;
    }

    public Set<Object> getPrivateCredentials() {
        return privCredentials;
    }

    @SuppressWarnings("unchecked")
    public <T> Set<T> getPublicCredentials(Class<T> c) {
        Set<T> result = new HashSet<>();
        for (Object cred : pubCredentials) {
            if (c.isInstance(cred)) {
                result.add((T) cred);
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public <T> Set<T> getPrivateCredentials(Class<T> c) {
        Set<T> result = new HashSet<>();
        for (Object cred : privCredentials) {
            if (c.isInstance(cred)) {
                result.add((T) cred);
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Subject that)) return false;
        return principals.equals(that.principals);
    }

    @Override
    public int hashCode() {
        return principals.hashCode();
    }

    @Override
    public String toString() {
        return "Subject(principals=" + principals + ")";
    }
}
