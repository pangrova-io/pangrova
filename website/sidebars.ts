import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

/**
 * Pangrova documentation sidebar.
 *
 * A single, progressive learning path:
 *   Introduction → Getting Started → SQL Reference → Architecture → Operations → Contributing
 *
 * Docs are read from the project-root `docs/` directory (see docusaurus.config.ts
 * `path: '../docs'`). `numberPrefixParser: true` strips numeric filename prefixes
 * from doc IDs, e.g. `01-overview.md` → `architecture/overview`,
 * `02-distributed-architecture.md` → `architecture/distributed-architecture`.
 *
 * Sidebar labels are set explicitly here so navigation stays consistent and fully
 * English regardless of a source file's H1 heading.
 */
const sidebars: SidebarsConfig = {
  docs: [
    {
      type: 'doc',
      id: 'guide/index',
      label: 'Introduction',
    },
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        {type: 'doc', id: 'guide/getting-started', label: 'Quickstart'},
        {type: 'doc', id: 'guide/clients', label: 'Clients (JDBC / REST / CLI)'},
        {type: 'doc', id: 'guide/connectors', label: 'Connectors & Catalogs'},
      ],
    },
    {
      type: 'category',
      label: 'SQL Reference',
      collapsed: false,
      items: [
        {type: 'doc', id: 'guide/sql-reference', label: 'SQL Reference'},
        {type: 'doc', id: 'guide/data-types', label: 'Data Types'},
        {
          type: 'category',
          label: 'Functions',
          link: {type: 'doc', id: 'guide/functions/index'},
          items: [
            {type: 'doc', id: 'guide/functions/math-functions', label: 'Math'},
            {type: 'doc', id: 'guide/functions/string-functions', label: 'String'},
            {type: 'doc', id: 'guide/functions/datetime-functions', label: 'Date & Time'},
            {type: 'doc', id: 'guide/functions/aggregate-functions', label: 'Aggregate'},
            {type: 'doc', id: 'guide/functions/window-functions', label: 'Window'},
            {type: 'doc', id: 'guide/functions/conditional-functions', label: 'Conditional'},
            {type: 'doc', id: 'guide/functions/array-map-struct-functions', label: 'Array / Map / Struct'},
            {type: 'doc', id: 'guide/functions/spatial-functions', label: 'Spatial'},
            {type: 'doc', id: 'guide/functions/vector-functions', label: 'Vector'},
          ],
        },
        {type: 'doc', id: 'guide/vector-search', label: 'Vector Search'},
        {type: 'doc', id: 'guide/ai-functions', label: 'AI Functions'},
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      link: {type: 'doc', id: 'architecture/README'},
      items: [
        {
          type: 'category',
          label: 'Core Engine',
          collapsed: false,
          items: [
            {type: 'doc', id: 'architecture/overview', label: 'System Overview'},
            {type: 'doc', id: 'architecture/distributed-architecture', label: 'Distributed Architecture'},
            {type: 'doc', id: 'architecture/query-execution', label: 'Query Execution'},
            {type: 'doc', id: 'architecture/query-optimization', label: 'Query Optimization'},
            {type: 'doc', id: 'architecture/native-layer', label: 'Native Execution Layer'},
          ],
        },
        {
          type: 'category',
          label: 'Data & Storage',
          items: [
            {type: 'doc', id: 'architecture/connector-design', label: 'Connector Design'},
            {type: 'doc', id: 'architecture/metadata-management', label: 'Metadata Management'},
            {type: 'doc', id: 'architecture/indexing-architecture', label: 'Indexing'},
            {type: 'doc', id: 'architecture/streaming', label: 'Streaming & CDC'},
            {type: 'doc', id: 'architecture/distributed-write-design', label: 'Distributed Write'},
          ],
        },
        {
          type: 'category',
          label: 'Platform Services',
          items: [
            {type: 'doc', id: 'architecture/resource-management', label: 'Resource Management'},
            {type: 'doc', id: 'architecture/resilience', label: 'Resilience'},
            {type: 'doc', id: 'architecture/cache-layer', label: 'Cache Layer'},
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
            {type: 'doc', id: 'architecture/ai-native-architecture', label: 'AI-Native Architecture'},
          ],
        },
        {
          type: 'category',
          label: 'Internals & Reference',
          items: [
            {type: 'doc', id: 'architecture/jni-bridge-design', label: 'JNI Bridge'},
            {type: 'doc', id: 'architecture/dynamic-filter-lifecycle', label: 'Dynamic Filter Lifecycle'},
            {type: 'doc', id: 'architecture/configuration-reference', label: 'Configuration Reference'},
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Operations',
      link: {type: 'doc', id: 'develop/README'},
      items: [
        {
          type: 'category',
          label: 'Deployment',
          items: [
            {type: 'doc', id: 'develop/deployment/README', label: 'Deployment Guide'},
            {type: 'doc', id: 'develop/deployment/DISTRIBUTED-DEPLOYMENT', label: 'Distributed Deployment'},
            {type: 'doc', id: 'develop/deployment/PRODUCTION-CHECKLIST', label: 'Production Checklist'},
          ],
        },
        {type: 'doc', id: 'develop/configuration/CONFIG', label: 'Configuration'},
        {type: 'doc', id: 'architecture/operations', label: 'Day-2 Operations & SOPs'},
        {
          type: 'category',
          label: 'Game Day',
          link: {type: 'doc', id: 'develop/game-day/README'},
          items: [
            {
              type: 'category',
              label: 'Scenarios',
              items: [
                {type: 'doc', id: 'develop/game-day/scenarios/gd-001-coordinator-failover', label: 'GD-001 Coordinator Failover'},
                {type: 'doc', id: 'develop/game-day/scenarios/gd-002-worker-crash-shuffle', label: 'GD-002 Worker Crash / Shuffle'},
                {type: 'doc', id: 'develop/game-day/scenarios/gd-003-metadata-partition', label: 'GD-003 Metadata Partition'},
                {type: 'doc', id: 'develop/game-day/scenarios/gd-004-network-partition', label: 'GD-004 Network Partition'},
                {type: 'doc', id: 'develop/game-day/scenarios/gd-005-disk-full-worker', label: 'GD-005 Disk-Full Worker'},
                {type: 'doc', id: 'develop/game-day/scenarios/gd-006-clock-skew', label: 'GD-006 Clock Skew'},
              ],
            },
            {
              type: 'category',
              label: 'Runbooks',
              items: [
                {type: 'doc', id: 'develop/game-day/runbooks/coordinator-failover-runbook', label: 'Coordinator Failover'},
                {type: 'doc', id: 'develop/game-day/runbooks/worker-crash-runbook', label: 'Worker Crash'},
                {type: 'doc', id: 'develop/game-day/runbooks/metadata-partition-runbook', label: 'Metadata Partition'},
                {type: 'doc', id: 'develop/game-day/runbooks/network-partition-runbook', label: 'Network Partition'},
                {type: 'doc', id: 'develop/game-day/runbooks/disk-full-runbook', label: 'Disk Full'},
                {type: 'doc', id: 'develop/game-day/runbooks/clock-skew-runbook', label: 'Clock Skew'},
              ],
            },
            {type: 'doc', id: 'develop/game-day/templates/post-mortem-template', label: 'Post-Mortem Template'},
            {type: 'doc', id: 'develop/game-day/reports/first-game-day', label: 'Report: First Game Day'},
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Contributing',
      items: [
        {type: 'doc', id: 'architecture/developer-guide', label: 'Developer Guide'},
        {type: 'doc', id: 'architecture/testing-framework', label: 'Testing Framework'},
      ],
    },
  ],
};

export default sidebars;
