import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

/**
 * Pangrova documentation sidebar.
 *
 * Structure: Introduction → Getting Started → SQL Reference → Data Types →
 * Indexing → Configuration → Deployment → Architecture → Operations → Contributing
 *
 * Docs are read from the project-root `docs/` directory (see docusaurus.config.ts
 * `path: '../docs'`). File names are stable, lowercase, and independent of sidebar
 * order; navigation order is defined only in this file.
 *
 * Sidebar labels are set explicitly here so navigation stays consistent and fully
 * English regardless of a source file's H1 heading.
 */
const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'doc',
      id: 'introduction',
      label: 'Introduction',
    },
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        {type: 'doc', id: 'getting-started/project-positioning', label: 'Positioning & Customers'},
        {type: 'doc', id: 'getting-started/capability-status', label: 'Current Capability Status'},
        {type: 'doc', id: 'getting-started/quickstart', label: 'Quickstart'},
        {
          type: 'category',
          label: 'Clients',
          items: [
            {type: 'doc', id: 'getting-started/clients/jdbc', label: 'JDBC'},
            {type: 'doc', id: 'getting-started/clients/rest-api', label: 'REST API'},
            {type: 'doc', id: 'getting-started/clients/rest-catalog-api', label: 'REST Catalog API'},
            {type: 'doc', id: 'getting-started/clients/cli', label: 'CLI'},
          ],
        },
        {
          type: 'category',
          label: 'Connectors',
          items: [
            {type: 'doc', id: 'getting-started/connectors/iceberg', label: 'Iceberg'},
            {type: 'doc', id: 'getting-started/connectors/paimon', label: 'Paimon'},
          ],
        },
        {
          type: 'category',
          label: 'Catalogs',
          items: [
            {type: 'doc', id: 'getting-started/catalogs/catalog-management', label: 'Catalog Management'},
            {type: 'doc', id: 'getting-started/catalogs/storage-backends', label: 'Storage Backends'},
            {type: 'doc', id: 'getting-started/catalogs/multi-catalog-example', label: 'Multi-Catalog Example'},
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'SQL Reference',
      collapsed: false,
      link: {type: 'doc', id: 'sql-reference/index'},
      items: [
        {type: 'doc', id: 'sql-reference/queries', label: 'Queries'},
        {type: 'doc', id: 'sql-reference/dml', label: 'DML'},
        {type: 'doc', id: 'sql-reference/ddl', label: 'DDL'},
        {type: 'doc', id: 'sql-reference/maintenance', label: 'Maintenance'},
        {type: 'doc', id: 'sql-reference/streaming', label: 'Streaming'},
        {type: 'doc', id: 'sql-reference/versioning', label: 'Versioning'},
        {type: 'doc', id: 'sql-reference/session', label: 'Session'},
        {
          type: 'category',
          label: 'Functions',
          link: {type: 'doc', id: 'sql-reference/functions/index'},
          items: [
            {type: 'doc', id: 'sql-reference/functions/math', label: 'Math'},
            {type: 'doc', id: 'sql-reference/functions/string', label: 'String'},
            {type: 'doc', id: 'sql-reference/functions/date-time', label: 'Date & Time'},
            {type: 'doc', id: 'sql-reference/functions/aggregate', label: 'Aggregate'},
            {type: 'doc', id: 'sql-reference/functions/window', label: 'Window'},
            {type: 'doc', id: 'sql-reference/functions/conditional', label: 'Conditional'},
            {type: 'doc', id: 'sql-reference/functions/array', label: 'Array'},
            {type: 'doc', id: 'sql-reference/functions/map', label: 'Map'},
            {type: 'doc', id: 'sql-reference/functions/struct', label: 'Struct'},
            {type: 'doc', id: 'sql-reference/functions/table-functions', label: 'Table Functions'},
            {type: 'doc', id: 'sql-reference/functions/spatial', label: 'Spatial'},
            {type: 'doc', id: 'sql-reference/functions/vector', label: 'Vector'},
            {type: 'doc', id: 'sql-reference/functions/vector-search', label: 'Vector Search'},
            {type: 'doc', id: 'sql-reference/functions/ai-functions', label: 'AI Functions'},
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Data Types',
      items: [
        {
          type: 'category',
          label: 'Numeric',
          items: [
            {type: 'doc', id: 'data-types/numeric/boolean', label: 'BOOLEAN'},
            {type: 'doc', id: 'data-types/numeric/tinyint', label: 'TINYINT'},
            {type: 'doc', id: 'data-types/numeric/smallint', label: 'SMALLINT'},
            {type: 'doc', id: 'data-types/numeric/integer', label: 'INT / INTEGER'},
            {type: 'doc', id: 'data-types/numeric/bigint', label: 'BIGINT'},
            {type: 'doc', id: 'data-types/numeric/float', label: 'FLOAT'},
            {type: 'doc', id: 'data-types/numeric/double', label: 'DOUBLE'},
            {type: 'doc', id: 'data-types/numeric/decimal', label: 'DECIMAL'},
          ],
        },
        {
          type: 'category',
          label: 'String',
          items: [
            {type: 'doc', id: 'data-types/string/varchar', label: 'VARCHAR'},
          ],
        },
        {
          type: 'category',
          label: 'Date & Time',
          items: [
            {type: 'doc', id: 'data-types/date-time/date', label: 'DATE'},
            {type: 'doc', id: 'data-types/date-time/timestamp', label: 'TIMESTAMP'},
            {type: 'doc', id: 'data-types/date-time/timestamp-ns', label: 'TIMESTAMP_NS'},
            {type: 'doc', id: 'data-types/date-time/literal-only-types', label: 'Literal-Only Types'},
          ],
        },
        {
          type: 'category',
          label: 'Complex',
          items: [
            {type: 'doc', id: 'data-types/complex/array', label: 'ARRAY'},
            {type: 'doc', id: 'data-types/complex/map', label: 'MAP'},
            {type: 'doc', id: 'data-types/complex/struct', label: 'STRUCT'},
          ],
        },
        {
          type: 'category',
          label: 'AI-Native',
          items: [
            {type: 'doc', id: 'data-types/ai-native/vector', label: 'VECTOR'},
            {type: 'doc', id: 'data-types/ai-native/file', label: 'FILE'},
          ],
        },
        {
          type: 'category',
          label: 'Semi-Structured',
          items: [
            {type: 'doc', id: 'data-types/semi-structured/variant', label: 'VARIANT'},
          ],
        },
        {
          type: 'category',
          label: 'Spatial',
          items: [
            {type: 'doc', id: 'data-types/spatial/geometry', label: 'GEOMETRY'},
            {type: 'doc', id: 'data-types/spatial/geography', label: 'GEOGRAPHY'},
          ],
        },
        {type: 'doc', id: 'data-types/binary', label: 'Binary'},
        {type: 'doc', id: 'data-types/type-casting', label: 'Type Casting'},
      ],
    },
    {
      type: 'category',
      label: 'Indexing',
      items: [
        {type: 'doc', id: 'indexing/index-ddl', label: 'Index DDL'},
        {
          type: 'category',
          label: 'Data-Skipping',
          items: [
            {type: 'doc', id: 'indexing/data-skipping/bloom-filter', label: 'BLOOM_FILTER'},
            {type: 'doc', id: 'indexing/data-skipping/ngram-bloom-filter', label: 'NGRAM_BLOOM_FILTER'},
            {type: 'doc', id: 'indexing/data-skipping/bitmap', label: 'BITMAP'},
            {type: 'doc', id: 'indexing/data-skipping/sparse', label: 'SPARSE'},
          ],
        },
        {
          type: 'category',
          label: 'Lookup',
          items: [
            {type: 'doc', id: 'indexing/lookup/btree', label: 'BTREE'},
            {type: 'doc', id: 'indexing/lookup/hash', label: 'HASH'},
            {type: 'doc', id: 'indexing/lookup/prefix', label: 'PREFIX'},
          ],
        },
        {type: 'doc', id: 'indexing/full-text', label: 'Full-Text'},
        {type: 'doc', id: 'indexing/containment', label: 'Containment'},
        {type: 'doc', id: 'indexing/spatial', label: 'Spatial'},
        {type: 'doc', id: 'indexing/vector', label: 'Vector'},
      ],
    },
    {
      type: 'category',
      label: 'Configuration',
      link: {type: 'doc', id: 'configuration/index'},
      items: [
        {type: 'doc', id: 'configuration/coordinator', label: 'Coordinator'},
        {type: 'doc', id: 'configuration/worker', label: 'Worker'},
        {type: 'doc', id: 'configuration/connectors', label: 'Connectors'},
        {type: 'doc', id: 'configuration/resources', label: 'Resources'},
        {type: 'doc', id: 'configuration/security', label: 'Security'},
        {type: 'doc', id: 'configuration/observability', label: 'Observability'},
        {type: 'doc', id: 'configuration/operations', label: 'Operations'},
        {type: 'doc', id: 'configuration/reference', label: 'Key Reference'},
      ],
    },
    {
      type: 'category',
      label: 'Deployment',
      items: [
        {type: 'doc', id: 'deployment/planning', label: 'Planning'},
        {type: 'doc', id: 'deployment/docker', label: 'Docker'},
        {type: 'doc', id: 'deployment/helm-kubernetes', label: 'Helm / Kubernetes'},
        {type: 'doc', id: 'deployment/distributed', label: 'Distributed'},
        {type: 'doc', id: 'deployment/production-checklist', label: 'Production Checklist'},
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      link: {type: 'doc', id: 'architecture/index'},
      items: [
        {
          type: 'category',
          label: 'Core Engine',
          collapsed: false,
          items: [
            {type: 'doc', id: 'architecture/overview', label: 'System Overview'},
            {type: 'doc', id: 'architecture/distributed', label: 'Distributed Architecture'},
            {type: 'doc', id: 'architecture/query-execution', label: 'Query Execution'},
            {type: 'doc', id: 'architecture/query-optimization', label: 'Query Optimization'},
            {type: 'doc', id: 'architecture/native-execution', label: 'Native Execution Layer'},
          ],
        },
        {
          type: 'category',
          label: 'Data & Storage',
          items: [
            {type: 'doc', id: 'architecture/connectors', label: 'Connector Design'},
            {type: 'doc', id: 'architecture/metadata', label: 'Metadata Management'},
            {type: 'doc', id: 'architecture/indexing', label: 'Indexing (Target)'},
            {type: 'doc', id: 'architecture/streaming', label: 'Streaming & CDC'},
            {type: 'doc', id: 'architecture/distributed-writes', label: 'Distributed Write'},
          ],
        },
        {
          type: 'category',
          label: 'Platform Services',
          items: [
            {type: 'doc', id: 'architecture/resource-management', label: 'Resource Management'},
            {type: 'doc', id: 'architecture/resilience', label: 'Resilience'},
            {type: 'doc', id: 'architecture/caching', label: 'Caching (Target)'},
            {type: 'doc', id: 'architecture/observability', label: 'Observability'},
            {type: 'doc', id: 'architecture/security', label: 'Security'},
            {type: 'doc', id: 'architecture/multi-tenancy', label: 'Multi-Tenancy'},
            {type: 'doc', id: 'architecture/data-governance', label: 'Data Governance'},
          ],
        },
        {
          type: 'category',
          label: 'AI-Native',
          items: [
            {type: 'doc', id: 'architecture/ai-native', label: 'AI-Native (Draft)'},
          ],
        },
        {
          type: 'category',
          label: 'Internals & Reference',
          items: [
            {type: 'doc', id: 'architecture/jni-bridge', label: 'JNI Bridge'},
            {type: 'doc', id: 'architecture/dynamic-filtering', label: 'Dynamic Filter Lifecycle'},
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      link: {type: 'doc', id: 'operations/index'},
      items: [
        {type: 'doc', id: 'operations/day-2', label: 'Day-2 Operations & SOPs'},
        {
          type: 'category',
          label: 'Game Day',
          link: {type: 'doc', id: 'operations/game-day/index'},
          items: [
            {
              type: 'category',
              label: 'Scenarios',
              items: [
                {type: 'doc', id: 'operations/game-day/scenarios/gd-001-coordinator-failover', label: 'GD-001 Coordinator Failover'},
                {type: 'doc', id: 'operations/game-day/scenarios/gd-002-worker-crash-shuffle', label: 'GD-002 Worker Crash / Shuffle'},
                {type: 'doc', id: 'operations/game-day/scenarios/gd-003-metadata-partition', label: 'GD-003 Metadata Partition'},
                {type: 'doc', id: 'operations/game-day/scenarios/gd-004-network-partition', label: 'GD-004 Network Partition'},
                {type: 'doc', id: 'operations/game-day/scenarios/gd-005-disk-full-worker', label: 'GD-005 Disk-Full Worker'},
                {type: 'doc', id: 'operations/game-day/scenarios/gd-006-clock-skew', label: 'GD-006 Clock Skew'},
              ],
            },
            {
              type: 'category',
              label: 'Runbooks',
              items: [
                {type: 'doc', id: 'operations/game-day/runbooks/coordinator-failover-runbook', label: 'Coordinator Failover'},
                {type: 'doc', id: 'operations/game-day/runbooks/worker-crash-runbook', label: 'Worker Crash'},
                {type: 'doc', id: 'operations/game-day/runbooks/metadata-partition-runbook', label: 'Metadata Partition'},
                {type: 'doc', id: 'operations/game-day/runbooks/network-partition-runbook', label: 'Network Partition'},
                {type: 'doc', id: 'operations/game-day/runbooks/disk-full-runbook', label: 'Disk Full'},
                {type: 'doc', id: 'operations/game-day/runbooks/clock-skew-runbook', label: 'Clock Skew'},
              ],
            },
            {type: 'doc', id: 'operations/game-day/templates/post-mortem-template', label: 'Post-Mortem Template'},
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Contributing',
      items: [
        {type: 'doc', id: 'contributing/developer-guide', label: 'Developer Guide'},
        {type: 'doc', id: 'contributing/testing', label: 'Testing Framework'},
        {type: 'doc', id: 'contributing/documentation', label: 'Documentation Guide'},
      ],
    },
  ],
};

export default sidebars;
