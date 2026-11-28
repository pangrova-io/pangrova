import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import styles from './index.module.css';

type IconName = 'bolt' | 'database' | 'sparkles' | 'network' | 'shield' | 'pulse';

const icons: Record<IconName, React.ReactNode> = {
  bolt: <path d="M13 2 4.5 13.5H11l-1 8.5 8.5-11.5H12l1-8.5Z" />,
  database: (
    <>
      <ellipse cx="12" cy="5.5" rx="7.5" ry="3" />
      <path d="M4.5 5.5v6c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3v-6" />
      <path d="M4.5 11.5v6c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3v-6" />
    </>
  ),
  sparkles: (
    <>
      <path d="M12 3.5 13.8 9l5.5 1.8-5.5 1.8L12 18l-1.8-5.4L4.7 10.8 10.2 9 12 3.5Z" />
      <path d="M19 15.5l.9 2.6 2.6.9-2.6.9-.9 2.6-.9-2.6-2.6-.9 2.6-.9.9-2.6Z" />
    </>
  ),
  network: (
    <>
      <rect x="3" y="3" width="6" height="6" rx="1.4" />
      <rect x="15" y="3" width="6" height="6" rx="1.4" />
      <rect x="9" y="15" width="6" height="6" rx="1.4" />
      <path d="M9 6h6M6 9v3h12V9M12 12v3" />
    </>
  ),
  shield: (
    <>
      <path d="M12 3 5 6v5.5c0 4.3 2.9 7.6 7 9.5 4.1-1.9 7-5.2 7-9.5V6l-7-3Z" />
      <path d="m9.2 12 2 2 3.8-4" />
    </>
  ),
  pulse: <path d="M3 12h4l2.5-6 4 12L16 9l2 3h3" />,
};

function FeatureIcon({name}: {name: IconName}) {
  return (
    <svg
      className={styles.featureIcon}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true">
      {icons[name]}
    </svg>
  );
}

const features: {
  icon: IconName;
  title: string;
  description: string;
  link: string;
  linkLabel: string;
}[] = [
  {
    icon: 'bolt',
    title: 'Native vectorized execution',
    description:
      'Calcite plans the query; C++20 and Velox execute relational work through JNI while Arrow carries columnar data across boundaries.',
    link: '/docs/architecture/native-execution/',
    linkLabel: 'Native execution',
  },
  {
    icon: 'database',
    title: 'Open lakehouse formats',
    description:
      'Query Apache Iceberg and Apache Paimon tables in place, or use the managed Lance connector. The versioned Connector SPI adds new formats and data sources without forking the engine.',
    link: '/docs/architecture/connectors/',
    linkLabel: 'Connector design',
  },
  {
    icon: 'sparkles',
    title: 'Analytics and AI in SQL',
    description:
      'Relational SQL, vector and full-text retrieval, and governed model invocation share one planning, execution, and governance path.',
    link: '/docs/architecture/ai-native/',
    linkLabel: 'AI-native architecture',
  },
  {
    icon: 'network',
    title: 'Distributed by design',
    description:
      'A Java Coordinator owns planning, admission, scheduling, and final catalog commit. Workers execute fragments and exchange data over Arrow Flight.',
    link: '/docs/architecture/distributed/',
    linkLabel: 'Distributed architecture',
  },
  {
    icon: 'shield',
    title: 'Governance and security',
    description:
      'Tenant identity, RBAC, row policies, column masks, audit, lineage, and quality rules bind to the same table identity used at execution.',
    link: '/docs/architecture/security/',
    linkLabel: 'Security model',
  },
  {
    icon: 'pulse',
    title: 'Operable platform',
    description:
      'Leases, fencing, resource queues, spilling, metadata caches, and OpenTelemetry instrumentation support day-2 operations on configured deployments.',
    link: '/docs/operations/day-2/',
    linkLabel: 'Day-2 operations',
  },
];

const ecosystem = [
  {
    label: 'Lakehouse',
    items: ['Apache Iceberg', 'Apache Paimon', 'Lance', 'S3', 'OSS', 'HDFS'],
  },
  {
    label: 'Engine',
    items: ['Apache Calcite', 'Velox', 'Apache Arrow', 'Arrow Flight', 'Substrait', 'JNI'],
  },
  {
    label: 'Platform',
    items: ['PostgreSQL', 'ZooKeeper', 'Kafka', 'Redis', 'OpenTelemetry', 'Prometheus'],
  },
];

const community = [
  {
    title: 'GitHub organization',
    description: 'Follow the project organization and repository update announcements.',
    href: 'https://github.com/pangrova-io',
    label: 'Open GitHub',
  },
  {
    title: 'Documentation',
    description: 'Start with the quickstart and architecture overview.',
    href: '/docs/',
    label: 'Read the docs',
  },
  {
    title: 'Roadmap',
    description: 'Review development priorities and open engineering items.',
    href: '/docs/getting-started/roadmap/',
    label: 'View the roadmap',
  },
];

const sqlExamples = {
  analytics: `SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue
FROM iceberg.sales.orders
WHERE order_date >= TIMESTAMP '2024-01-01'
GROUP BY region
ORDER BY revenue DESC;`,
  ai: `-- A four-dimensional vector column; ANN acceleration
-- requires a matching plan and index binding.
SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist
FROM products
ORDER BY dist ASC
LIMIT 10;`,
  timeTravel: `-- Read the table at a previous point in time.
SELECT * FROM iceberg.analytics.events
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';`,
};

function Hero() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={styles.hero}>
      <div className={styles.heroGlow} aria-hidden="true" />
      <div className={styles.heroGrid} aria-hidden="true" />
      <div className="container">
        <div className={styles.heroContent}>
          <div className={styles.heroBadge}>
            <span className={styles.heroBadgeDot} />
            Open source · Apache License 2.0
          </div>
          <div className={styles.heroBrand}>
            <img src="img/logo-transparent.png" alt="Pangrova logo" className={styles.heroLogo} />
            <span className={styles.heroWordmark}>Pangrova</span>
          </div>
          <h1 className={styles.heroTitle}>{siteConfig.tagline}</h1>
          <p className={styles.heroSubtitle}>
            {siteConfig.title} is a distributed SQL engine for lakehouse analytics and AI
            retrieval. It combines Apache Calcite planning with C++20 / Velox execution,
            Arrow Flight exchange, and connectors for Apache Iceberg, Apache Paimon, and
            managed Lance datasets.
          </p>
          <div className={styles.buttons}>
            <Link className="button button--primary button--lg" to="/docs/getting-started/quickstart/">
              Get started
            </Link>
            <Link
              className={clsx('button button--outline button--lg', styles.secondaryButton)}
              href="https://github.com/pangrova-io">
              View on GitHub
            </Link>
          </div>
          <div className={styles.heroMeta}>
            <span>JDBC</span>
            <span>REST API</span>
            <span>CLI</span>
            <span>Iceberg</span>
            <span>Paimon</span>
            <span>AI SQL</span>
          </div>
        </div>
      </div>
    </header>
  );
}

function Features() {
  return (
    <section className={styles.section}>
      <div className="container">
        <div className={styles.sectionHeading}>
          <span className={styles.eyebrow}>Why Pangrova</span>
          <h2 className={styles.sectionTitle}>One engine for analytics and retrieval</h2>
          <p className={styles.sectionLead}>
            Query existing lakehouse tables without migration, execute relational work natively,
            and keep governance attached to the data that already exists.
          </p>
        </div>
        <div className={styles.featureGrid}>
          {features.map((feature) => (
            <article key={feature.title} className={styles.featureCard}>
              <div className={styles.featureIconWrap}>
                <FeatureIcon name={feature.icon} />
              </div>
              <h3 className={styles.featureTitle}>{feature.title}</h3>
              <p className={styles.featureText}>{feature.description}</p>
              <Link className={styles.textLink} to={feature.link}>
                {feature.linkLabel} →
              </Link>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

function Architecture() {
  return (
    <section className={clsx(styles.section, styles.altSection)}>
      <div className="container">
        <div className={styles.sectionHeading}>
          <span className={styles.eyebrow}>Architecture</span>
          <h2 className={styles.sectionTitle}>Control plane and execution plane</h2>
          <p className={styles.sectionLead}>
            The Coordinator owns parsing, planning, authorization, admission, scheduling, and the
            final fenced catalog commit. Workers execute through JNI and exchange data over Arrow
            Flight; connectors isolate lakehouse metadata and storage access.
          </p>
        </div>
        <div className={styles.archFrame}>
          <img
            src="img/architecture-overview.png"
            alt="Pangrova architecture: clients, streaming and AI ingestion, Java Coordinator, durable control state, Workers with native execution, Connector SPI, lakehouse storage, and platform services"
            className={styles.archImage}
          />
        </div>
        <div className={styles.archLinks}>
          <Link to="/docs/architecture/overview/">System overview →</Link>
          <Link to="/docs/architecture/query-execution/">Query execution →</Link>
          <Link to="/docs/architecture/distributed/">Distributed architecture →</Link>
        </div>
      </div>
    </section>
  );
}

function QuickStart() {
  return (
    <section className={styles.section}>
      <div className="container">
        <div className={styles.split}>
          <div>
            <span className={styles.eyebrow}>Quick start</span>
            <h2 className={styles.sectionTitleLeft}>Run your first query</h2>
            <p className={styles.sectionLeadLeft}>
              Build the pinned Iceberg SDK, the native library, and the Java modules, then launch a
              standalone Coordinator and Worker. The first native build compiles external
              dependencies and can take substantial time.
            </p>
            <ol className={styles.steps}>
              <li>
                <span>1</span>
                <div>
                  Install JDK 17, Maven 3.9+, Gradle 8.14.4, Python 3.10+, CMake, Ninja, and a C++20
                  compiler.
                </div>
              </li>
              <li>
                <span>2</span>
                <div>Build the pinned dependencies and the native library.</div>
              </li>
              <li>
                <span>3</span>
                <div>
                  Package the reactor and start <code>bin/pangrova-standalone.sh</code>.
                </div>
              </li>
              <li>
                <span>4</span>
                <div>
                  Connect with the CLI or JDBC and run SQL. Review the{' '}
                  <Link to="/docs/getting-started/quickstart/">full quickstart</Link> for
                  configuration and troubleshooting.
                </div>
              </li>
            </ol>
          </div>
          <div className={styles.codeCard}>
            <Tabs>
              <TabItem value="standalone" label="Standalone" default>
                <CodeBlock language="bash">
{`.pangrova/scripts/build-iceberg-sdk-candidate.sh
./pangrova-worker-native/bin/build-native.sh
mvn clean package -DskipTests
./bin/pangrova-standalone.sh`}
                </CodeBlock>
              </TabItem>
              <TabItem value="cli" label="CLI">
                <CodeBlock language="bash">
{`./bin/pangrova-cli.sh --host localhost --port 8080

SELECT 1 + 1 AS result;`}
                </CodeBlock>
              </TabItem>
              <TabItem value="jdbc" label="JDBC">
                <CodeBlock language="java">
{`String url = "jdbc:pangrova://localhost:8080/iceberg_catalog";
try (Connection conn = DriverManager.getConnection(url, props);
     Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery("SELECT 1 + 1 AS result")) {
    rs.next();
    System.out.println(rs.getInt("result"));
}`}
                </CodeBlock>
              </TabItem>
              <TabItem value="docker" label="Docker">
                <CodeBlock language="bash">
{`cd deploy/docker
docker compose -f docker-compose-cluster.yml up -d
docker compose -f docker-compose-cluster.yml ps`}
                </CodeBlock>
              </TabItem>
            </Tabs>
          </div>
        </div>
      </div>
    </section>
  );
}

function SqlShowcase() {
  return (
    <section className={clsx(styles.section, styles.altSection)}>
      <div className="container">
        <div className={styles.sectionHeading}>
          <span className={styles.eyebrow}>SQL surface</span>
          <h2 className={styles.sectionTitle}>Analytics, retrieval, and versioned reads</h2>
        </div>
        <div className={styles.sqlCard}>
          <Tabs>
            <TabItem value="analytics" label="Lakehouse analytics" default>
              <CodeBlock language="sql">{sqlExamples.analytics}</CodeBlock>
            </TabItem>
            <TabItem value="ai" label="Vector search">
              <CodeBlock language="sql">{sqlExamples.ai}</CodeBlock>
            </TabItem>
            <TabItem value="travel" label="Time travel">
              <CodeBlock language="sql">{sqlExamples.timeTravel}</CodeBlock>
            </TabItem>
          </Tabs>
          <Link className={styles.textLink} to="/docs/sql-reference/">
            Explore the SQL reference →
          </Link>
        </div>
      </div>
    </section>
  );
}

function Ecosystem() {
  return (
    <section className={styles.section}>
      <div className="container">
        <div className={styles.sectionHeading}>
          <span className={styles.eyebrow}>Ecosystem</span>
          <h2 className={styles.sectionTitle}>Built with proven lakehouse components</h2>
        </div>
        <div className={styles.ecosystemGrid}>
          {ecosystem.map((group) => (
            <div key={group.label} className={styles.ecosystemGroup}>
              <h3>{group.label}</h3>
              <div className={styles.chips}>
                {group.items.map((item) => (
                  <span key={item} className={styles.chip}>
                    {item}
                  </span>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function Community() {
  return (
    <section className={clsx(styles.section, styles.communitySection)}>
      <div className="container">
        <div className={styles.sectionHeading}>
          <span className={styles.eyebrow}>Community</span>
          <h2 className={styles.sectionTitle}>Build Pangrova with us</h2>
          <p className={styles.sectionLead}>
            Pangrova is developed in the open. Bring real workloads, correctness expectations, and
            deployment constraints so that support claims are grounded in evidence.
          </p>
        </div>
        <div className={styles.communityGrid}>
          {community.map((item) => (
            <Link key={item.title} className={styles.communityCard} to={item.href}>
              <h3>{item.title}</h3>
              <p>{item.description}</p>
              <span>{item.label} →</span>
            </Link>
          ))}
        </div>
      </div>
    </section>
  );
}

export default function Home(): React.JSX.Element {
  return (
    <Layout
      title="Distributed SQL for Open Lakehouses"
      description="Pangrova is an open-source distributed SQL engine for open lakehouses, combining Apache Calcite planning, C++20 / Velox execution, Arrow Flight exchange, and connectors for Apache Iceberg and Apache Paimon.">
      <Hero />
      <main>
        <Features />
        <Architecture />
        <QuickStart />
        <SqlShowcase />
        <Ecosystem />
        <Community />
      </main>
    </Layout>
  );
}
