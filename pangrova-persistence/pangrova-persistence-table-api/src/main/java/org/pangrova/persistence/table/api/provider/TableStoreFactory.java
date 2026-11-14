package org.pangrova.persistence.table.api.provider;

import org.pangrova.common.api.annotation.Experimental;
import javax.sql.DataSource;
import java.util.Optional;

/** Creates a consistent set of table repositories. The supplying process owns any JDBC pool. */
@Experimental
public interface TableStoreFactory {
    String identifier();

    TableStores create(Optional<DataSource> dataSource);
}
