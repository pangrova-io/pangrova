package org.pangrova.persistence.jdbc.api.security.policy;

import org.pangrova.common.api.annotation.Internal;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope.TablePermission;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.security.api.mask.ColumnMask;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/** Current persisted facts on the supplied connection; never commits, rolls back or closes it. */
@Internal
public interface CurrentPolicyReader {
    List<RowPolicy> readRows(Connection connection, TablePermission permission) throws SQLException;

    List<ColumnMask> readMasks(Connection connection, TablePermission permission, String column) throws SQLException;
}
