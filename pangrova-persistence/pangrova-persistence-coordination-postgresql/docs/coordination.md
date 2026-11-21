# Coordination PostgreSQL provider

`pangrova-persistence-coordination-postgresql` owns the real JDBC implementations in
`org.pangrova.persistence.coordination.postgresql.{membership,lease,admission,error}`.
The contracts live in `pangrova-persistence-coordination-api`; the old generic
Persistence JDBC module no longer owns these classes or re-exports their packages.

The provider implements Worker and Coordinator registries, distributed leases and
atomic live query reservations over a caller-supplied `DataSource`. Its production
closure needs only the domain contracts/values, Common, JDBC support, Jackson and
SLF4J. It has no dependency on business Core, Protocol, generic Persistence backends
or the ZooKeeper SDK. PostgreSQL and Flyway test dependencies are test scoped.

Server assembly owns the data source and schema initialization. Operations close
borrowed connections/statements, never the data source. All DDL belongs to
`pangrova-schema-migrations`; V106 initializes the existing admission tables for a
new database. Admission construction validates counter/owner consistency and fails
if schema is missing or state is inconsistent. It does not build an alternative
schema or repair unknown reservations. No historical data conversion is provided.

Worker heartbeat, refresh and withdrawal retain exact process identity checks.
Lease acquisition/renewal/release retain database-issued epochs and authoritative
clock checks after row locking and before commit. Existing transaction SQL and
lock ordering are preserved. The registry and lease providers do not share a hidden
transaction or close one another's resources.

Tests use actual isolated PostgreSQL schemas initialized by Testing Kit's canonical
migration fixture, including concurrent lease/registration operations, lock-clock
races, exact Worker identity and rollback failures. `JdbcCoordinationClasspathIsolationTest`
checks the provider owner's actual test classpath excludes Coordination Core and
Metadata implementation classes. Historical migration tests remain historical
validation, not a new deployment migration feature.
