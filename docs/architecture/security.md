---
title: "Security"
sidebar_label: "Security"
---

# Pangrova — Security

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** Authentication, authorization, row/column-level access control, data masking, and audit logging.

## 1. Security Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Security Architecture                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Security layers:                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Layer              │ Responsibility              │ Implementation      │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Authentication     │ Identity verification       │ LDAP/OAuth2/Custom  │   │
│  │  Authorization      │ Permission control          │ RBAC + ABAC         │   │
│  │  Row-Level Security │ Row-level filtering         │ Dynamic predicate   │   │
│  │  Column Masking     │ Column-level masking        │ Expression rewrite  │   │
│  │  Encryption         │ Data encryption             │ TLS + storage enc.  │   │
│  │  Audit              │ Audit logging               │ End-to-end tracing  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Authentication

### 2.1 Authentication Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Authentication Flow                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Client                                                                          │
│    │ Credentials (Username/Password, Token, OAuth2 Token)                      │
│    ▼                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Authentication Manager                                │   │
│  │                                                                          │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│  │  │   LDAP      │  │ Bypass/Dev  │  │   OAuth2    │  │   Custom    │   │   │
│  │  │ Authenticator│ │ Authenticator│ │ Authenticator│ │ Authenticator│   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │   │
│  │                                                                          │   │
│  │  Auth success → generate SessionToken → bind UserIdentity              │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Authentication SPI

```java
/**
 * Authenticator SPI
 */
//
public interface Authenticator extends AutoCloseable {
    String getName();
    void init(Map<String, String> config);
    AuthenticationResult authenticate(AuthenticationRequest request);
    boolean supports(AuthenticationRequest request);
    boolean isHealthy();
    @Override void close();
}

/**
 * Authentication request
 */
public class AuthenticationRequest {
    private final String username;
    private final String password;
    private final String token;
    private final byte[] kerberosTicket;
    private final Map<String, String> extraCredentials;
    private final String clientIp;
}

/**
 * User identity
 */
public class UserIdentity {
    private final String principal;           // Principal identifier
    private final Set<String> groups;         // Groups the user belongs to
    private final Map<String, String> attributes;  // Extended attributes
    private final Instant authenticatedAt;
}

/**
 * LDAP authenticator implementation
 */
public class LdapAuthenticator implements Authenticator {

    private final LdapConfig config;
    private final LdapContextFactory contextFactory;

    @Override
    public AuthenticationResult authenticate(AuthenticationRequest request) {
        try {
            DirContext ctx = contextFactory.createContext(
                config.getUserDnPattern().replace("{0}", request.getUsername()),
                request.getPassword());

            // Fetch user groups
            Set<String> groups = fetchUserGroups(ctx, request.getUsername());

            return AuthenticationResult.success(
                new UserIdentity(request.getUsername(), groups, Map.of()));

        } catch (AuthenticationException e) {
            return AuthenticationResult.failure("Invalid credentials");
        }
    }
}

/**
 * OAuth2 authenticator implementation
 */
public class OAuth2Authenticator implements Authenticator {

    @Override
    public AuthenticationResult authenticate(AuthenticationRequest request) {
        // 1. Validate Access Token
        TokenInfo tokenInfo = tokenValidator.validate(request.getToken());

        // 2. Fetch user info
        UserInfo userInfo = userInfoClient.getUserInfo(request.getToken());

        return AuthenticationResult.success(
            new UserIdentity(userInfo.getSubject(), userInfo.getGroups(),
                Map.of("email", userInfo.getEmail())));
    }
}
```

---

## 3. Authorization

### 3.1 RBAC + ABAC Hybrid Model

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Authorization Model                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  RBAC (Role-Based Access Control):                                              │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  User → Role → Permission                                                │   │
│  │                                                                          │   │
│  │  Example:                                                                │   │
│  │  alice → data_analyst → SELECT on warehouse.*                           │   │
│  │  bob → data_engineer → SELECT, INSERT, CREATE on warehouse.*            │   │
│  │  admin → admin_role → ALL PRIVILEGES                                    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  ABAC (Attribute-Based Access Control):                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Attribute-based dynamic policies                                        │   │
│  │                                                                          │   │
│  │  Example:                                                                │   │
│  │  • user.department == 'finance' → can access finance.* tables          │   │
│  │  • user.level >= 3 → can view the salary column                        │   │
│  │  • request.time in business_hours → access allowed                     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Permission Model

```java
/**
 * Privilege types
 */
public enum Privilege {
    // Table-level privileges
    SELECT, INSERT, UPDATE, DELETE, TRUNCATE, DROP, ALTER,
    // Schema-level privileges
    CREATE_TABLE, CREATE_VIEW, DROP_SCHEMA,
    // Catalog-level privileges
    CREATE_SCHEMA, DROP_CATALOG,
    // System-level privileges
    EXECUTE_QUERY, KILL_QUERY, VIEW_QUERY, SET_SESSION
}

/**
 * Permission checker
 *
 * Unified interception point design:
 * AccessControl has a single call entry point in the query lifecycle: MetadataService.resolveTable().
 *
 * Call chain:
 *   Query Parse → Semantic Analysis → MetadataService.resolveTable()
 *     → AccessControl.checkCanAccessCatalog()
 *     → AccessControl.checkCanSelectFromTable() / checkCanSelectFromColumns()
 *     → AccessControl.getRowFilter() → injected into Calcite RelNode
 *     → AccessControl.getColumnMask() → injected into Calcite RelNode
 *   → Query Optimization → Execution (AccessControl no longer called)
 *
 * AccessControl MUST NOT be re-invoked on the Worker side during Query Execution (query-execution.md).
 * Reason: Workers only execute the Substrait Plan dispatched by the Coordinator, and the Plan already
 * contains the RLS filter conditions and Column Mask expressions. Re-checking on the Worker side would cause:
 *   1. Performance overhead (every Task querying the PostgreSQL permission tables)
 *   2. Consistency risk (permission changes during query execution causing some Tasks to fail)
 *   3. Responsibility confusion (Workers should focus only on data processing, not permissions)
 *
 * Exception: The Coordinator's DDL operations (CREATE/DROP/ALTER) call AccessControl directly,
 * without going through MetadataService.resolveTable().
 *
 * See also: metadata.md §3.1 MetadataService
 */

public interface AccessControl {

    /** Check Catalog access permission */
    void checkCanAccessCatalog(UserIdentity user, String catalog);

    /** Check Schema access permission */
    void checkCanAccessSchema(UserIdentity user, CatalogSchemaName schema);

    /** Check table permissions */
    void checkCanSelectFromTable(UserIdentity user, CatalogSchemaTableName table);
    void checkCanInsertIntoTable(UserIdentity user, CatalogSchemaTableName table);
    void checkCanDeleteFromTable(UserIdentity user, CatalogSchemaTableName table);
    void checkCanDropTable(UserIdentity user, CatalogSchemaTableName table);

    /** Check column permissions */
    void checkCanSelectFromColumns(UserIdentity user, CatalogSchemaTableName table,
                                   Set<String> columns);

    /** Get row-level filter condition */
    Optional<Expression> getRowFilter(UserIdentity user, CatalogSchemaTableName table);

    /** Get column masking rule */
    Optional<Expression> getColumnMask(UserIdentity user, CatalogSchemaTableName table,
                                       String column);
}
```

### 3.3 Permission Storage

```sql
-- Role definitions
CREATE TABLE roles (
    role_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    role_name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User-role mapping
CREATE TABLE user_roles (
    user_principal VARCHAR(255) NOT NULL,
    role_id BIGINT NOT NULL,
    granted_by VARCHAR(255),
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_principal, role_id)
);

-- Permission grants
CREATE TABLE privileges (
    privilege_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    role_id BIGINT NOT NULL,
    privilege_type VARCHAR(50) NOT NULL,      -- SELECT, INSERT, etc.
    object_type VARCHAR(50) NOT NULL,         -- CATALOG, SCHEMA, TABLE, COLUMN
    catalog_name VARCHAR(255),
    schema_name VARCHAR(255),
    table_name VARCHAR(255),
    column_name VARCHAR(255),
    with_grant_option BOOLEAN DEFAULT FALSE,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_role_object (role_id, object_type, catalog_name, schema_name, table_name)
);
```

### 3.4 Metadata-table read authority

The implemented metadata suffixes (`$files`, `$entries`, `$manifests`,
`$snapshots`, `$history`, `$tags`, `$options`, `$schemas`, `$branches`) require
full SELECT authority on the base table. A grant on a suffix name does not
permit reading the base table's metadata. `AccessControlEnforcer` invokes
`TableAccessControl.checkCanReadTableMetadata` for planned scans, explicit
metadata SELECT checks and metadata column checks. The policy subject is always
the authenticated reader.

`DynamicRbacAccessControl` denies these unfiltered metadata reads when a base
row filter or column mask applies to that subject. Metadata counts, paths,
partition values and summaries cannot be transformed by applying a base-row
predicate to the metadata schema. This restriction also covers aggregate-only
queries and SHOW statements routed through these relations. Ordinary base
queries continue through the existing row-filter and mask transformations.

The metadata check resolves roles once for both grants and policy applicability;
revoking a role during the check cannot erase an applicable mask after the
SELECT check succeeds. `EnrichedColumnMaskStore.getTableMasks` enumerates table
policies independently of projection and current schema, including policies on
historical column names. Mask selection preserves normal enabled, role, priority
and exemption semantics. Removing the applicable policy restores metadata
eligibility on the next check. Policy-store failures propagate without allowing
a raw read.

[Athena's documented Iceberg restrictions](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
provide a precedent for denying sensitive metadata when row or cell filters
apply. Pangrova deliberately applies one eligibility rule to its whole
implemented metadata family, rather than treating individual metadata columns
as policy-safe without a connector-specific proof. This section describes the
current read boundary; it does not claim a transaction spanning policy changes
and an already running data-plane query.

---

## 4. Row-Level Security

### 4.1 RLS Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Row-Level Security                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Principle: automatically inject row filter conditions during the query optimization phase │
│                                                                                  │
│  Original query:                                                                 │
│  SELECT * FROM orders WHERE amount > 1000                                       │
│                                                                                  │
│  RLS policy (user.region = row.region):                                          │
│  User alice belongs to region='US'                                               │
│                                                                                  │
│  Rewritten query:                                                                │
│  SELECT * FROM orders WHERE amount > 1000 AND region = 'US'                     │
│                                                                                  │
│  Execution flow:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. SQL Parse → AST                                                      │   │
│  │  2. Semantic Analysis → identify involved tables                        │   │
│  │  3. RLS Check → query the row_policies table                            │   │
│  │  4. Filter Injection → add filter condition on TableScan                │   │
│  │  5. Optimize → normal optimization flow                                 │   │
│  │  6. Execute → user sees only rows matching the policy                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 RLS Policy Definition

```sql
-- Row-level security policy table
CREATE TABLE row_policies (
    policy_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    policy_name VARCHAR(255) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,

    -- Policy expression (SQL fragment)
    -- May reference: current_user(), current_groups(), user_attribute('key')
    filter_expression TEXT NOT NULL,

    -- Applicable scope
    apply_to_roles TEXT,                      -- JSON array of role names, null = all
    enabled BOOLEAN DEFAULT TRUE,
    priority INT DEFAULT 0,                   -- priority when multiple policies apply

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_policy (catalog_name, schema_name, table_name, policy_name)
);

-- Example policies
INSERT INTO row_policies (policy_name, catalog_name, schema_name, table_name, filter_expression)
VALUES
-- Users can only see data from their own department
('department_isolation', 'iceberg', 'hr', 'employees',
 "department = user_attribute('department')"),

-- Can only see orders from their own region
('region_isolation', 'iceberg', 'sales', 'orders',
 "region IN (SELECT region FROM user_regions WHERE user_id = current_user())"),

-- Sensitive data visible only to high-level users
('salary_protection', 'iceberg', 'hr', 'salaries',
 "CASE WHEN user_attribute('level') >= 3 THEN TRUE ELSE employee_id = current_user() END");
```

### 4.3 RLS Implementation

```java
/**
 * Row-level security filter
 */
public class RowLevelSecurityFilter {

    private final RowPolicyStore policyStore;
    private final ExpressionParser expressionParser;

    /**
     * Get the row filter condition for a table
     */
    public Optional<Expression> getRowFilter(UserIdentity user, CatalogSchemaTableName table) {
        List<RowPolicy> policies = policyStore.getPolicies(
            table.getCatalog(), table.getSchema(), table.getTable());

        if (policies.isEmpty()) {
            return Optional.empty();
        }

        // Filter policies applicable to the current user
        List<RowPolicy> applicablePolicies = policies.stream()
            .filter(p -> p.isEnabled())
            .filter(p -> p.appliesTo(user))
            .sorted(Comparator.comparingInt(RowPolicy::getPriority).reversed())
            .toList();

        if (applicablePolicies.isEmpty()) {
            return Optional.empty();
        }

        // Combine multiple policies with AND
        Expression combined = applicablePolicies.stream()
            .map(p -> parseAndBind(p.getFilterExpression(), user))
            .reduce((a, b) -> new LogicalExpression(AND, a, b))
            .orElseThrow();

        return Optional.of(combined);
    }
```

### 4.4 RLS Multi-Policy Conflict Resolution Rules

When a single table has multiple RLS policies, explicit merge rules are required. Pangrova adopts an **AND semantics + priority ordering** model.

#### Merge semantics: AND vs OR

| Scenario | Merge method | Reason |
|------|----------|------|
| Multiple policies on the same table | **AND** (all policies must be satisfied simultaneously) | Security first: multiple policies represent isolation requirements across multiple dimensions and must all be satisfied |
| A single user matches multiple roles, each with a different policy | **AND** (intersection of per-role policies) | Least-privilege principle: the user can only see the intersection of all role policies |
| Policy with `apply_to_roles = null` (global policy) | **AND**-merged with role-specific policies | A global policy is a baseline constraint and cannot be relaxed by role policies |

> **Design decision**: The core reason for choosing AND over OR is security. OR semantics means access is granted if any single policy is satisfied, which lets a permissive policy override a strict one and creates a data-leak risk. AND semantics ensures all security constraints take effect simultaneously.

#### Role of priority

The `row_policies.priority` field does not affect AND/OR semantics; instead it controls the evaluation order and short-circuit behavior of policies:

| priority value | Behavior |
|---|---|
| High priority (larger value) | Evaluated first. If this policy evaluates to `FALSE`, short-circuit and skip subsequent policies (performance optimization) |
| Low priority (smaller value) | Evaluated later. Executed only when higher-priority policies did not short-circuit |
| Same priority | Evaluated in lexicographic order of `policy_name` (ensures determinism) |

#### Conflict scenario example

```sql
-- Scenario: the employees table has two RLS policies

-- Policy 1: department isolation (priority=10)
-- filter: department = user_attribute('department')

-- Policy 2: region isolation (priority=5)
-- filter: region IN (SELECT region FROM user_regions WHERE user_id = current_user())

-- User alice: department='engineering', region='US'
-- Merged filter condition (AND):
--   department = 'engineering' AND region IN ('US')
-- alice can only see employees in the engineering department and US region

-- User bob: department='finance', but has no user_regions record
-- Merged filter condition (AND):
--   department = 'finance' AND region IN ()  <- empty set
-- bob sees no data (under AND semantics, an empty-set IN condition filters out all rows)
```

#### Special-case handling

| Case | Behavior | Note |
|------|------|------|
| User matches no policy | Returns `Optional.empty()` -> no filter | No policy = no restriction (access must be controlled via RBAC privileges) |
| All matching policies are disabled | Same as above | Disabled policies do not participate in merging |
| Policy expression fails to parse | Throws `PangrovaException(INTERNAL_ERROR)` -> query fails | Security first: on a policy error, deny access rather than degrading to no filter |
| Policy references a nonexistent user_attribute | That attribute returns the empty string `""` | No exception thrown, but the filter condition may not match (fails safe) |
| Policy contains a subquery (e.g., `IN (SELECT ...)`) | The subquery is expanded during the Calcite optimization phase | Subquery results are cached per-query and not shared across queries |

```java

    /**
     * Parse and bind the user context
     */
    private Expression parseAndBind(String expression, UserIdentity user) {
        Expression parsed = expressionParser.parse(expression);

        // Replace built-in functions
        return parsed.accept(new ExpressionRewriter() {
            @Override
            public Expression visitFunctionCall(FunctionCall call) {
                switch (call.getName()) {
                    case "current_user":
                        return new StringLiteral(user.getPrincipal());
                    case "current_groups":
                        return new ArrayLiteral(user.getGroups());
                    case "user_attribute":
                        String key = ((StringLiteral) call.getArguments().get(0)).getValue();
                        return new StringLiteral(user.getAttributes().getOrDefault(key, ""));
                    default:
                        return call;
                }
            }
        });
    }
}
```

---

## 5. Column Masking

### 5.1 Masking Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Column Masking                                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Principle: replace expressions of sensitive columns during the query optimization phase   │
│                                                                                  │
│  Original query:                                                                 │
│  SELECT name, email, phone, salary FROM employees                               │
│                                                                                  │
│  Masking policies:                                                               │
│  • email: partially hidden → j***@example.com                                   │
│  • phone: show last four → ****1234                                             │
│  • salary: NULL for unauthorized users                                          │
│                                                                                  │
│  Rewritten query (for a regular user):                                           │
│  SELECT name,                                                                    │
│         CONCAT(SUBSTR(email,1,1), '***@', SUBSTR(email, INSTR(email,'@')+1)),   │
│         CONCAT('****', RIGHT(phone, 4)),                                        │
│         NULL as salary                                                          │
│  FROM employees                                                                  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Masking Policy Definition

```sql
-- Column masking policy table
CREATE TABLE column_masks (
    mask_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    mask_name VARCHAR(255) NOT NULL,
    catalog_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255) NOT NULL,

    -- Masking expression (SQL fragment; {column} represents the original column)
    mask_expression TEXT NOT NULL,

    -- Applicability scope (non-applicable roles can see the original value)
    apply_to_roles TEXT,                      -- JSON array, null = all except exempt
    exempt_roles TEXT,                        -- JSON array of exempt roles

    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_mask (catalog_name, schema_name, table_name, column_name, mask_name)
);

-- Built-in masking functions
-- mask_email(email) → j***@example.com
-- mask_phone(phone) → ****1234
-- mask_card(card_number) → ****-****-****-1234
-- mask_name(name) → Z*
-- mask_null() → NULL
-- mask_hash(value) → SHA256 hash
-- mask_range(number, precision) → 10000-20000

-- Example policies
INSERT INTO column_masks (mask_name, catalog_name, schema_name, table_name, column_name,
                          mask_expression, exempt_roles)
VALUES
('email_mask', 'iceberg', 'hr', 'employees', 'email',
 "mask_email({column})", '["admin", "hr_manager"]'),

('phone_mask', 'iceberg', 'hr', 'employees', 'phone',
 "mask_phone({column})", '["admin"]'),

('salary_mask', 'iceberg', 'hr', 'employees', 'salary',
 "CASE WHEN user_attribute('level') >= 3 THEN {column} ELSE NULL END", '["admin"]'),

('ssn_mask', 'iceberg', 'hr', 'employees', 'ssn',
 "CONCAT('***-**-', RIGHT({column}, 4))", '["admin"]'),

('card_mask', 'iceberg', 'finance', 'payments', 'card_number',
 "mask_card({column})", '["admin", "finance_admin"]');
```

### 5.3 Masking Implementation

```java
/**
 * Column masking processor
 */
public class ColumnMaskingProcessor {

    private final ColumnMaskStore maskStore;

    /**
     * Get the masking expression for a column
     */
    public Optional<Expression> getColumnMask(UserIdentity user,
                                               CatalogSchemaTableName table,
                                               String column) {
        List<ColumnMask> masks = maskStore.getMasks(
            table.getCatalog(), table.getSchema(), table.getTable(), column);

        for (ColumnMask mask : masks) {
            if (!mask.isEnabled()) continue;

            // Check whether exempt
            if (mask.isExempt(user)) {
                return Optional.empty();  // Return the original value
            }

            // Check whether applicable
            if (mask.appliesTo(user)) {
                Expression maskExpr = parseMaskExpression(mask.getMaskExpression(), column);
                return Optional.of(maskExpr);
            }
        }

        return Optional.empty();
    }

    /**
     * Built-in masking function implementations
     */
    public static class MaskFunctions {

        public static String maskEmail(String email) {
            if (email == null || !email.contains("@")) return email;
            int atIndex = email.indexOf('@');
            return email.charAt(0) + "***" + email.substring(atIndex);
        }

        public static String maskPhone(String phone) {
            if (phone == null || phone.length() < 4) return phone;
            return "****" + phone.substring(phone.length() - 4);
        }

        public static String maskCard(String card) {
            if (card == null || card.length() < 4) return card;
            String last4 = card.substring(card.length() - 4);
            return "****-****-****-" + last4;
        }

        public static String maskName(String name) {
            if (name == null || name.isEmpty()) return name;
            return name.charAt(0) + "*".repeat(name.length() - 1);
        }
    }
}
```

---

### 5.4 Security Policy Change Events

Security policy changes need to publish events externally so that components such as the result cache can trigger invalidation.

```java
/**
 * Security policy change event
 */
public sealed interface SecurityPolicyChangeEvent
        permits RlsPolicyChanged, ColumnMaskChanged, RoleAssignmentChanged {
    String catalog();
    String schema();
    String table();
}

public record RlsPolicyChanged(String catalog, String schema, String table)
        implements SecurityPolicyChangeEvent {}

public record ColumnMaskChanged(String catalog, String schema, String table)
        implements SecurityPolicyChangeEvent {}

public record RoleAssignmentChanged(String userPrincipal)
        implements SecurityPolicyChangeEvent {
    @Override
    public String catalog() { return ""; }
    @Override
    public String schema() { return ""; }
    @Override
    public String table() { return ""; }
}

/**
 * Event publisher
 */
public interface SecurityPolicyEventPublisher {
    void publish(SecurityPolicyChangeEvent event);
}
```

Consumer contracts:
- Query Result Cache subscribes and performs cache invalidation (see [query-optimization.md](./query-optimization.md)).
- The metadata cache may optionally subscribe, for cache cleanup after privilege changes.

## 6. Audit Log

### 6.1 Audit Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Audit Architecture                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Audit scope:                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Category          │ Events                                              │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Authentication    │ LOGIN_SUCCESS, LOGIN_FAILURE, LOGOUT, TOKEN_REFRESH│   │
│  │  Authorization     │ ACCESS_DENIED, PRIVILEGE_CHECK                     │   │
│  │  Query             │ QUERY_SUBMITTED, QUERY_COMPLETED, QUERY_FAILED,   │   │
│  │                    │ QUERY_CANCELLED                                    │   │
│  │  Data Access       │ TABLE_READ, TABLE_WRITE, TABLE_DELETE             │   │
│  │  DDL               │ TABLE_CREATE, TABLE_DROP, TABLE_ALTER,            │   │
│  │                    │ SCHEMA_CREATE, SCHEMA_DROP, VIEW_CREATE, VIEW_DROP│   │
│  │  Admin             │ USER_CREATE, USER_DROP, USER_ALTER,               │   │
│  │                    │ ROLE_CREATE, ROLE_DROP, ROLE_GRANT, ROLE_REVOKE,  │   │
│  │                    │ POLICY_CREATE, POLICY_DROP, POLICY_ALTER,         │   │
│  │                    │ CONFIG_CHANGE                                     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Audit flow:                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Operation → AuditInterceptor → AuditEvent → AuditLogger → Storage     │   │
│  │                                                                          │   │
│  │  Storage Options:                                                        │   │
│  │  • PostgreSQL (structured queries)                                       │   │
│  │  • Elasticsearch (full-text search)                                     │   │
│  │  • Kafka → HDFS (large-scale archival)                                  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Audit Event Model



```java
/**
 * Audit event
 */
public class AuditEvent {
    private final String eventId;             // UUID
    private final AuditEventType eventType;
    private final Instant timestamp;

    // User information
    private final String principal;
    private final String clientIp;
    private final String userAgent;

    //
    // Distributed tracing correlation (see observability.md §4)
    private final String traceId;             // OpenTelemetry trace ID for cross-referencing with Jaeger
    private final String spanId;              // OpenTelemetry span ID

    // Query information
    private final String queryId;
    private final String sql;
    private final String catalog;
    private final String schema;

    // Accessed objects
    private final List<AccessedObject> accessedObjects;

    // Result
    private final boolean success;
    private final String errorMessage;

    // Resource consumption
    private final long durationMs;
    private final long rowsRead;
    private final long rowsWritten;
    private final long bytesRead;

    // Extended attributes
    private final Map<String, String> properties;
}

/**
 * Accessed object
 */
public class AccessedObject {
    private final ObjectType objectType;      // TABLE, VIEW, COLUMN
    private final String catalog;
    private final String schema;
    private final String table;
    private final Set<String> columns;
    private final AccessType accessType;      // READ, WRITE, DDL
}

/**
 * Audit event types
 */
public enum AuditEventType {
    // Authentication
    LOGIN_SUCCESS, LOGIN_FAILURE, LOGOUT, TOKEN_REFRESH,

    // Authorization
    ACCESS_DENIED, PRIVILEGE_CHECK,

    // Query
    QUERY_SUBMITTED, QUERY_COMPLETED, QUERY_FAILED, QUERY_CANCELLED,

    // Data access
    TABLE_READ, TABLE_WRITE, TABLE_DELETE,

    // DDL
    TABLE_CREATE, TABLE_DROP, TABLE_ALTER,
    SCHEMA_CREATE, SCHEMA_DROP,
    VIEW_CREATE, VIEW_DROP,

    // Administration
    USER_CREATE, USER_DROP, USER_ALTER,
    ROLE_CREATE, ROLE_DROP, ROLE_GRANT, ROLE_REVOKE,
    POLICY_CREATE, POLICY_DROP, POLICY_ALTER,
    CONFIG_CHANGE
}
```

### 6.3 Audit Storage



```sql
-- Audit log table (PostgreSQL)
CREATE TABLE audit_logs (
    event_id VARCHAR(36) PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMP(6) NOT NULL,

    -- User information
    principal VARCHAR(255) NOT NULL,
    client_ip VARCHAR(45),
    user_agent VARCHAR(500),

    --
    -- Distributed tracing correlation
    trace_id VARCHAR(32),                     -- OpenTelemetry trace ID (hex)
    span_id VARCHAR(16),                      -- OpenTelemetry span ID (hex)

    -- Query information
    query_id VARCHAR(36),
    sql_text TEXT,
    sql_hash VARCHAR(64),                     -- SQL fingerprint

    -- Accessed objects
    catalog_name VARCHAR(255),
    schema_name VARCHAR(255),
    table_name VARCHAR(255),
    accessed_columns JSON,

    -- Result
    success BOOLEAN NOT NULL,
    error_code VARCHAR(50),
    error_message TEXT,

    -- Resource consumption
    duration_ms BIGINT,
    rows_read BIGINT,
    rows_written BIGINT,
    bytes_read BIGINT,

    -- Extension
    properties JSON,

    -- Indexes
    INDEX idx_time (event_time),
    INDEX idx_principal (principal, event_time),
    INDEX idx_query (query_id),
    INDEX idx_table (catalog_name, schema_name, table_name, event_time),
    INDEX idx_type (event_type, event_time)
) PARTITION BY RANGE (UNIX_TIMESTAMP(event_time)) (
    -- Partition by month, retain 12 months
);

-- Audit log archival configuration
-- Hot data: PostgreSQL (30 days)
-- Warm data: Elasticsearch (90 days)
-- Cold data: HDFS/S3 (permanent)
```

### 6.4 Audit Query API

```sql
-- Query all operations of a given user
SELECT * FROM audit_logs
WHERE principal = 'alice'
  AND event_time > NOW() - INTERVAL 7 DAY
ORDER BY event_time DESC;

-- Query access records of a sensitive table
SELECT principal, event_type, sql_text, event_time
FROM audit_logs
WHERE table_name = 'salaries'
  AND event_time > NOW() - INTERVAL 30 DAY
ORDER BY event_time DESC;

-- Query failed access attempts
SELECT principal, client_ip, error_message, event_time
FROM audit_logs
WHERE success = FALSE
  AND event_type = 'ACCESS_DENIED'
  AND event_time > NOW() - INTERVAL 1 DAY;

-- Aggregate query volume per user
SELECT principal, COUNT(*) as query_count,
       SUM(rows_read) as total_rows_read
FROM audit_logs
WHERE event_type = 'QUERY_COMPLETED'
  AND event_time > NOW() - INTERVAL 7 DAY
GROUP BY principal
ORDER BY query_count DESC;
```

---

## 7. Encryption

### 7.1 Encryption Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Encryption Architecture                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Encryption layers:                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Layer                │ Method            │ Implementation                │   │
│  │  ─────────────────────────────────────────────────────────────────────  │   │
│  │  Transport encryption │ TLS 1.3           │ All network communication     │   │
│  │  Storage encryption   │ AES-256-GCM       │ S3 SSE / HDFS encryption zone │   │
│  │  Column-level         │ AES-256-GCM       │ Sensitive columns encrypted   │   │
│  │  Key management       │ KMS               │ AWS KMS / HashiCorp Vault     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Transport Encryption Configuration



```yaml
# coordinator-config.yaml — TLS configuration
# Proposed config path: security.encryption.tls.*
security:
  encryption:
    tls:
      enabled: true
      keystore-path: /etc/pangrova/ssl/keystore.jks
      keystore-password: ${SSL_KEYSTORE_PASSWORD}
      truststore-path: /etc/pangrova/ssl/truststore.jks
      truststore-password: ${SSL_TRUSTSTORE_PASSWORD}
      protocol: TLSv1.3
      ciphers:
        - TLS_AES_256_GCM_SHA384
        - TLS_CHACHA20_POLY1305_SHA256

# Internal communication encryption (Worker-to-Worker, Coordinator-to-Worker)
    internal:
      mutual-auth: true  # Mutual authentication
```

### 7.3 Key Management

```java
/**
 * Key management SPI
 */
public interface KeyManager {
    /** Get the data encryption key */
    SecretKey getDataEncryptionKey(String keyId);

    /** Create a new key */
    String createKey(String alias, KeySpec spec);

    /** Rotate a key */
    String rotateKey(String keyId);

    /** Encrypt a data key (encrypted with the master key) */
    byte[] encryptDataKey(SecretKey dataKey, String masterKeyId);

    /** Decrypt a data key */
    SecretKey decryptDataKey(byte[] encryptedKey, String masterKeyId);
}

/**
 * AWS KMS implementation
 */
public class AwsKmsKeyManager implements KeyManager {
    private final KmsClient kmsClient;

    @Override
    public SecretKey getDataEncryptionKey(String keyId) {
        GenerateDataKeyResponse response = kmsClient.generateDataKey(
            GenerateDataKeyRequest.builder()
                .keyId(keyId)
                .keySpec(DataKeySpec.AES_256)
                .build());
        return new SecretKeySpec(response.plaintext().asByteArray(), "AES");
    }
}
```

---

## 8. Security Configuration Summary

### 8.1 Query-Level Token Lifecycle Management

Long-running queries (such as large table scans or complex JOINs) may encounter token expiration during execution. Pangrova adopts a query-level token binding strategy:

**Design principle**: bind the token at query submission; a valid token is not required during the query lifecycle.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     Query-Level Token Lifecycle                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Token handling flow:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. Client submits a query (POST /v1/query/submit)                             │   │
│  │     → Coordinator validates the token (authentication)                   │   │
│  │     → Extract UserIdentity (principal, groups, attributes)               │   │
│  │     → Bind UserIdentity to QueryContext (original token not retained)     │   │
│  │                                                                          │   │
│  │  2. During query execution                                               │   │
│  │     → All permission checks use QueryContext.userIdentity, not the token  │   │
│  │     → Worker-to-Worker uses an internal Service Token (not user token)    │   │
│  │     → Even if the user token expires, the query keeps executing           │   │
│  │                                                                          │   │
│  │  3. Client fetches results (Arrow Flight DoGet)                          │   │
│  │     → Output Worker validates the Flight Ticket (query_id), not user token │   │
│  │     → Flight Ticket validity = query lifecycle + client_idle_timeout      │   │
│  │                                                                          │   │
│  │  4. Client queries state (GET /v1/query/{queryId}/state)                 │   │
│  │     → A valid token or Session Token is required                         │   │
│  │     → If expired, the Client SDK auto-refreshes and retries               │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Token refresh strategy:                                                         │
│  • The Client SDK proactively refreshes before expiry (TTL * 0.8)               │
│  • A failed refresh does not affect submitted queries (bound to UserIdentity)   │
│  • A failed refresh affects new submissions and state queries (token required)  │
│                                                                                  │
│  Internal Service Token:                                                         │
│  • Coordinator → Worker communication uses a Service Token (not user token)     │
│  • The Service Token is issued by the Coordinator, TTL = 24h, auto-rotated       │
│  • Workers validate the Service Token signature, not the user identity          │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```java
/**
 * Query-level token binding
 */
public class QueryTokenBinder {

    /**
     * At query submission: validate token → extract identity → bind to QueryContext
     * Query execution thereafter no longer depends on the original token
     */
    public QueryContext bindToken(AuthenticationResult authResult, QueryRequest request) {
        UserIdentity identity = authResult.getUserIdentity();

        return QueryContext.builder()
            .queryId(generateQueryId())
            .userIdentity(identity)           // Bind identity, do not retain the token
            .authenticatedAt(Instant.now())
            .sessionProperties(request.getSessionProperties())
            .build();
    }
}
```

### 8.2 Security Configuration Reference

```yaml
# security-config.yaml
security:
  # Authentication configuration
  authentication:
    type: ldap  # ldap, oauth2, kerberos, custom
    ldap:
      url: ldap://ldap.example.com:389
      base-dn: dc=example,dc=com
      user-dn-pattern: uid={0},ou=users
      group-search-base: ou=groups
    oauth2:
      issuer-uri: https://auth.example.com
      client-id: pangrova
      client-secret: ${OAUTH_CLIENT_SECRET}

  # Authorization configuration
  authorization:
    type: file  # file, database, ranger
    file:
      path: /etc/pangrova/access-control.json
    database:
      url: jdbc:postgresql://localhost:5432/pangrova_security

  # Row-level security
  row-level-security:
    enabled: true
    cache-ttl: 5m

  # Column masking
  column-masking:
    enabled: true
    cache-ttl: 5m

  # Audit
  audit:
    enabled: true
    storage: postgresql  # postgresql, elasticsearch, kafka
    postgresql:
      url: jdbc:postgresql://localhost:5432/pangrova_audit
    retention-days: 90

  # Encryption — proposed path: security.encryption.tls.* (see §7.2)
  #
  encryption:
    tls:
      enabled: true
      protocol: TLSv1.3
      ciphers:
        - TLS_AES_256_GCM_SHA384
        - TLS_CHACHA20_POLY1305_SHA256
    storage:
      enabled: true
      key-provider: aws-kms
      master-key-id: alias/pangrova-master
```


---

## 9. Secret Management

Pangrova enforces a **zero-plaintext-credential** policy. All sensitive configuration values (passwords, secrets, keys) must use secret reference syntax. Plaintext credentials are rejected at startup.

> **See also:** [Security configuration](../configuration/security.md) for the
> currently verified startup keys and validation behavior.

### 9.1 Supported Secret Providers

| Provider | Syntax | Description | Use Case |
|----------|--------|-------------|----------|
| **Environment Variable** | `${ENV:VAR_NAME}` | Reads from OS environment variable | CI/CD pipelines, container orchestration |
| **HashiCorp Vault** | `${VAULT:path#field}` | Fetches from Vault KV v2 engine | Production secrets with audit trail |
| **AWS KMS** | `${KMS:key-id}` | Decrypts via AWS KMS (or compatible cloud KMS) | Cloud-native deployments, envelope encryption |
| **File Reference** | `${FILE:path}` | Reads from local file (first line, trimmed) | Kubernetes Secret volume mounts |

### 9.2 Secret Reference Syntax

```yaml
# Environment variable — simplest, suitable for dev/CI
s3:
  access-key: ${ENV:AWS_ACCESS_KEY_ID}

# HashiCorp Vault — recommended for production
s3:
  secret-key: ${VAULT:secret/pangrova/s3#key}

# AWS KMS — cloud-native envelope encryption
security:
  ldap:
    bind-password: ${KMS:alias/pangrova-ldap}

# File reference — Kubernetes Secret mount
security:
  oauth2:
    client-secret: ${FILE:/run/secrets/oauth2}
```

**Resolution order:** At startup, the configuration loader resolves all `${...}` references before the application context initializes. If any provider is unreachable, startup fails with a clear error message identifying the unresolvable reference.

### 9.3 Key Rotation Strategy

Pangrova uses **TTL-based auto-refresh** for secret rotation:

| Provider | Rotation Support | Default TTL | Behavior |
|----------|-----------------|-------------|----------|
| ENV | Manual only | — | Requires process restart to pick up changes |
| VAULT | Auto-refresh | 1 hour | Background thread re-fetches on TTL expiry |
| KMS | Auto-refresh | 1 hour | Background thread re-decrypts on TTL expiry |
| FILE | File-watch | 30 seconds | Watches file modification time, reloads on change |

**Auto-refresh flow (Vault / KMS):**

```
1. Startup: resolve secret → cache value with TTL
2. Background scheduler checks TTL every 30s
3. TTL expired → re-fetch from provider
4. Success → update cached value, reset TTL
5. Failure → log warning, retain previous value, retry on next cycle
6. 3 consecutive failures → emit CRITICAL alert via observability pipeline
```

**Configuration:**

```yaml
secret-management:
  vault:
    address: https://vault.example.com:8200
    auth-method: kubernetes          # kubernetes, token, approle
    ttl: 1h                          # rotation interval
    retry-interval: 30s
    max-retry: 3
  kms:
    region: us-east-1
    ttl: 1h
    retry-interval: 30s
    max-retry: 3
  file:
    watch-interval: 30s
```

### 9.4 Key Lifecycle

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        Secret Lifecycle                                   │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Creation          Resolution         Rotation          Revocation       │
│  ─────────         ──────────         ────────          ──────────       │
│  Admin provisions  Config loader      TTL-based         Admin rotates    │
│  secret in         resolves ${...}    auto-refresh      master secret    │
│  Vault/KMS/ENV     at startup         re-fetches        in provider      │
│       │                 │             from provider          │           │
│       ▼                 ▼                  │                 ▼           │
│  Secret stored     Value cached       ┌───┴───┐        Old secret       │
│  in provider       in memory          │Success│        invalidated       │
│                         │             └───┬───┘        by provider       │
│                         ▼                 ▼                  │           │
│                    Application        Update cache          ▼           │
│                    uses secret        + reset TTL       Next rotation    │
│                                                         picks up new    │
│                                           │             value via TTL    │
│                                      ┌────┴────┐                        │
│                                      │ Failure │                        │
│                                      └────┬────┘                        │
│                                           ▼                             │
│                                      Retain previous                    │
│                                      value + alert                      │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

**Lifecycle phases:**

| Phase | Actor | Action |
|-------|-------|--------|
| **Creation** | Ops/Admin | Provision secret in external provider (Vault, KMS, ENV, file) |
| **Resolution** | Config Loader | Resolve `${...}` references at startup; fail-fast on unresolvable |
| **Rotation** | Secret Manager | TTL-based background refresh (Vault/KMS); file-watch (FILE); manual restart (ENV) |
| **Revocation** | Ops/Admin | Rotate master secret in provider; Pangrova picks up new value on next TTL cycle |

**Security invariants:**
- Secrets are **never** written to disk, logs, or diagnostic dumps
- In-memory secret values are stored in dedicated buffers cleared on rotation
- All secret access is recorded in the audit log (Section 6) as `CONFIG_SECRET_ACCESS` events

---

## 10. Cross-Reference: Result Cache Security Isolation

> **See also:** [`query-optimization.md`](query-optimization.md) — Section 3.2 (QueryFingerprintCalculator) and Section 3.3 (Cache Invalidation Triggers)

The query result cache includes security dimensions in its fingerprint to prevent cross-boundary data leakage. Specifically:

- **Cache fingerprint** incorporates `tenant_id`, `user_principal`, `active_roles_hash`, `rls_policy_hash`, and `column_mask_hash`, ensuring that cached results are never shared across different security contexts.
- **Cache invalidation** is triggered automatically when security policies change:
  - RLS policy change → invalidates all cache entries for the affected table
  - Column mask change → invalidates all cache entries for the affected table
  - Role assignment change → invalidates all cache entries for the affected user
- The `SecurityPolicyChangeListener` (defined in this module) publishes policy change events that the cache layer subscribes to for proactive invalidation.

This design ensures that the result cache respects all security boundaries defined in this document (authentication, RBAC, RLS, column masking) without requiring callers to manually manage cache coherence.

---



## 11. Observability Integration

This section defines the metrics and tracing integration points for the security layer. For the full observability design, see [observability.md](./observability.md).

### 11.1 Metrics

| Metric | Source Component | Description |
|--------|-----------------|-------------|
| `pangrova.security.auth.attempts` | Authenticator | Authentication attempts |
| `pangrova.security.auth.failures` | Authenticator | Authentication failures |
| `pangrova.security.auth.latency` | Authenticator | Authentication latency |
| `pangrova.security.authz.checks` | AccessControl | Authorization check count |
| `pangrova.security.authz.denials` | AccessControl | Authorization denials |
| `pangrova.security.authz.latency` | AccessControl | Authorization check latency |
| `pangrova.security.rls.filters_injected` | AccessControl | RLS filters injected |
| `pangrova.security.column_masks.applied` | AccessControl | Column masks applied |
| `pangrova.security.audit.events` | AuditLogger | Audit events recorded |

### 11.2 Tracing Spans

Security operations are children of the `query.optimize` span:

- `security.authenticate` — Authentication (child of `query.execute`)
- `security.authorize` — Authorization check (child of `query.optimize`)
- `security.rls.inject` — RLS filter injection (child of `query.optimize`)
- `security.mask.apply` — Column mask application (child of `query.optimize`)

### 11.3 Log Events

- Authentication success/failure events (INFO/WARN)
- Authorization denial events with principal and resource (WARN)
- Security policy change events (INFO)
- Audit log write events (DEBUG)

See [14 §7](./observability.md) for structured logging format.



> **SPI registration:** The implemented extension points are `Authenticator` and
> `KeyManager`. `Authenticator` works with `AuthenticationRequest`, exposes
> `authenticate`, `supports`, `isHealthy`, `init`, and `close`; `KeyManager` owns
> data-key creation, lookup, rotation, and envelope encryption. The interfaces in
> `pangrova-security` are authoritative for their exact signatures.


> **Audit implementation:** Security events use `AuditEventEmitter`. A separate
> resource-audit logger and the unified `audit_events` storage model described in
> the observability target design are not currently implemented.


> **Configuration status:** Only the keys listed in
> [Security configuration](../configuration/security.md) are verified runtime
> configuration. The broader RLS, masking, audit, and secret-management YAML in
> this document is target design.


> **ColumnMaskStore Interface (§5.3)**:
> ```java
> public interface ColumnMaskStore {
>     Optional<ColumnMask> getMask(String table, String column, SecurityContext ctx);
>     void createOrUpdateMask(String table, String column, String maskExpr, String tag);
>     void removeMask(String table, String column);
>     List<ColumnMask> listMasks(String table);
> }
> ```


> **Security Error Codes**: Security-related error codes are defined in [resilience.md §2.2](./resilience.md) at range 1800–1899: `AUTHENTICATION_FAILED(1801)`, `TOKEN_EXPIRED(1802)`, `RLS_POLICY_ERROR(1803)`, `COLUMN_MASK_ERROR(1804)`, `AUDIT_WRITE_FAILED(1805)`. These codes cover authentication failures, token expiry, RLS/masking policy errors, and audit write failures. All security components (Authenticator, AccessControl, AuditLogger) should throw `PangrovaException` with the corresponding error code from this range.
