package org.pangrova.persistence.security.postgresql.grant;

import org.pangrova.security.api.grant.PrivilegeMutation;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/** Length-framed canonical request equality, without Java serialization or lossy hashes. */
final class PrivilegeMutationEncoder {
    private PrivilegeMutationEncoder() {}

    static byte[] encode(PrivilegeMutation mutation) {
        try {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(1);
                text(out, mutation.operation().name());
                text(out, mutation.scope().objectType().name());
                text(out, mutation.scope().catalog());
                text(out, mutation.scope().schema());
                text(out, mutation.scope().object());
                out.writeBoolean(mutation.withGrantOption());
                text(out, mutation.revokeBehavior().name());
                var privileges = mutation.privileges().stream().map(Enum::name).sorted().toList();
                out.writeInt(privileges.size());
                for (var privilege : privileges) text(out, privilege);
                var roles = mutation.roles().stream().sorted().toList();
                out.writeInt(roles.size());
                for (var role : roles) text(out, role);
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
    }

    private static void text(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else {
            byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
            out.writeInt(encoded.length);
            out.write(encoded);
        }
    }
}
