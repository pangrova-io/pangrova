import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import styles from './index.module.css';

const features = [
  {
    icon: '⚡',
    title: 'Native Execution',
    description: 'C++20 / Velox vectorized execution through JNI, with Arrow interoperability. Performance must be measured on the target workload.',
  },
  {
    icon: '🔓',
    title: 'Open Formats',
    description: 'Built-in Apache Iceberg and Paimon connectors, with a pluggable Connector SPI for additional table formats and data sources. See documented format and operation limits.',
  },
  {
    icon: '🤖',
    title: 'AI-Native SQL',
    description: 'Vector, text, and model-function components in SQL. Query shape, index artifacts, and configured external model providers determine availability.',
  },
  {
    icon: '📈',
    title: 'Distributed Architecture',
    description: 'Coordinator / Worker execution with lease, fencing, and resource components. Capacity, recovery, and HA require deployment qualification.',
  },
  {
    icon: '🛡️',
    title: 'Security Components',
    description: 'Authentication, authorization, masking, row filters, and audit components. Configure and validate the runtime path for your deployment.',
  },
  {
    icon: '🔭',
    title: 'Observability',
    description: 'Tracing, metrics, and structured-logging providers and instrumentation for configured deployments.',
  },
];

const techStack = [
  'Apache Calcite',
  'Meta Velox',
  'Apache Arrow',
  'Substrait',
  'Apache Iceberg',
  'Apache Paimon',
  'Arrow Flight',
  'OpenTelemetry',
];

const sqlExamples = {
  analytics: `SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue
FROM iceberg.sales.orders
WHERE order_date >= TIMESTAMP '2024-01-01'
GROUP BY region
ORDER BY revenue DESC;`,
  ai: `-- Example for a four-dimensional vector column.
-- ANN acceleration requires a matching plan and index binding.
SELECT id, l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) AS dist
FROM products
ORDER BY l2_distance(embedding, ARRAY[1.0, 0.0, 0.0, 0.0]) ASC
LIMIT 10;`,
  timeTravel: `-- Query data as it existed at a specific point in time
SELECT * FROM iceberg.analytics.events
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';`,
};

function HeroSection() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <img src="img/logo.png" alt="Pangrova Logo" className={styles.heroLogo} />
        <h1 className={styles.heroTitle}>{siteConfig.title}</h1>
        <p className={styles.heroSubtitle}>{siteConfig.tagline}</p>
        <p className={styles.heroDescription}>
          Query open lakehouse tables with an open-source SQL engine built on
          Apache Calcite, Arrow, and Velox. Explore relational analytics and SQL
          primitives for vector search, full-text retrieval, and AI model invocation.
        </p>
        <p>
          Production qualification remains open. See{' '}
          <Link to="/docs/getting-started/capability-status" style={{color: 'white', textDecoration: 'underline'}}>
            current capability status and known gaps
          </Link>.
        </p>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/getting-started/quickstart">
            Get Started →
          </Link>
          <Link
            className="button button--outline button--lg"
            style={{color: 'white', borderColor: 'white', marginLeft: '1rem'}}
            href="https://github.com/pangrova-io/pangrova">
            GitHub ★
          </Link>
        </div>
      </div>
    </header>
  );
}

function FeaturesSection() {
  return (
    <section className={styles.features}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Why Pangrova?</h2>
        <div className={styles.featureGrid}>
          {features.map((feature, idx) => (
            <div key={idx} className={clsx('card', styles.featureCard)}>
              <div className={styles.featureIcon}>{feature.icon}</div>
              <h3>{feature.title}</h3>
              <p>{feature.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function ArchitectureSection() {
  return (
    <section className={styles.architectureSection}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Architecture</h2>
        <CodeBlock language="text">
{`Client -- HTTP control --> Coordinator
  parse / authorize / plan / schedule / metadata commit
                        |
                        | HTTP task control
                        v
Worker JVM -- JNI --> Native Worker (C++ / Velox)
  provider and          relational execution / retrieval
  connector callbacks   Native Flight shuffle / results
                        |
                        v
             Output Worker -- Flight --> Client`}
        </CodeBlock>
        <p>
          Main relational path; current CDC writes remain Coordinator-local.
          See the <Link to="/docs/architecture/overview">system overview</Link> and{' '}
          <Link to="/docs/getting-started/capability-status">capability boundaries</Link>.
        </p>
      </div>
    </section>
  );
}

function CodeShowcase() {
  return (
    <section className={styles.codeSection}>
      <div className="container">
        <h2 className={styles.sectionTitle}>SQL in Action</h2>
        <div className={styles.codeGrid}>
          <div>
            <h4>📊 Lakehouse Analytics</h4>
            <CodeBlock language="sql">{sqlExamples.analytics}</CodeBlock>
          </div>
          <div>
            <h4>🤖 AI-Native Search</h4>
            <CodeBlock language="sql">{sqlExamples.ai}</CodeBlock>
          </div>
          <div>
            <h4>⏰ Time Travel</h4>
            <CodeBlock language="sql">{sqlExamples.timeTravel}</CodeBlock>
          </div>
        </div>
      </div>
    </section>
  );
}

function TechStackSection() {
  return (
    <section className={styles.techSection}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Built On</h2>
        <div className={styles.techBadges}>
          {techStack.map((tech, idx) => (
            <span key={idx} className={styles.techBadge}>{tech}</span>
          ))}
        </div>
      </div>
    </section>
  );
}

function QuickStartSection() {
  return (
    <section className={styles.quickStart}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Quick Start</h2>
        <p>
          Requires JDK 17, Maven 3.9+, and the Native build prerequisites. Configure
          metadata, security, and storage before launch; follow the{' '}
          <Link to="/docs/getting-started/quickstart">full quickstart</Link>.
          The build below skips tests and does not qualify a release.
        </p>
        <CodeBlock language="bash">
{`git clone https://github.com/pangrova-io/pangrova.git
cd pangrova
./pangrova-worker-native/bin/setup-native-build-env.sh
./pangrova-worker-native/bin/build-native.sh
mvn clean install -DskipTests
./bin/pangrova-standalone.sh          # Start engine
./bin/pangrova-cli.sh                 # Interactive SQL`}
        </CodeBlock>
      </div>
    </section>
  );
}

export default function Home(): React.JSX.Element {
  return (
    <Layout
      title="Distributed SQL for Open Lakehouses"
      description="Pangrova is an extensible open-source distributed SQL engine for open lakehouses, built with Calcite, Arrow, and Velox for native vectorized analytics and AI SQL primitives."
    >
      <HeroSection />
      <main>
        <FeaturesSection />
        <ArchitectureSection />
        <CodeShowcase />
        <TechStackSection />
        <QuickStartSection />
      </main>
    </Layout>
  );
}
