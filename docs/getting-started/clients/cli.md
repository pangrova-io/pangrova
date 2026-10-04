---
title: "CLI"
description: "Interactive SQL shell and script mode"
---

# CLI

Use the Pangrova CLI for interactive queries and scripted SQL execution.


## CLI

The Pangrova CLI provides an interactive SQL shell.

### Launch

```bash
bin/pangrova-cli.sh --host localhost --port 8080
```

### Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host <host>` | `localhost` | Coordinator host |
| `--port <port>` | `8080` | Coordinator port |
| `--catalog <name>` | — | Default catalog |
| `--query-timeout <duration>` | `30m` | Query polling timeout (for example `30s`, `5m`, `1h`) |
| `--token <token>` | — | Bearer authentication; mutually exclusive with user/password |
| `--user <user>` / `--password <password>` | — | UTF-8 Basic authentication; both must be supplied |
| `--ssl <true/false>` | `false` | Use TLS for REST and require the actual result endpoint to advertise TLS |
| `--ssl-trust-store <path>` / `--ssl-trust-store-password <password>` | — | JKS/PKCS12 trust-store settings for REST HTTPS only |
| `--result-tls-root-ca <path>` | — | PEM result-channel trust roots; requires the actual endpoint to advertise TLS |
| `--result-tls-certificate-chain <path>` / `--result-tls-private-key <path>` | — | PEM client certificate and key for result-channel mutual TLS; both and explicit result trust roots are required |

Result-channel PEM settings are independent of REST trust stores and do not
change the REST URL scheme. Certificate and hostname verification remain enabled;
the client never upgrades or rewrites an advertised plaintext result endpoint.
Closing a client with a live result consumer or reader fails visibly, seals new
fetches, and retains its owners; retry close after the actual stream exits.

Use TLS for credential-bearing connections. Command-line credentials can be
visible in process listings; do not include real secrets in saved commands or logs.
The shell consumes a readable RUNNING result stream through EOF before waiting
for FINISHED. The same query deadline also bounds result reads; failed, cancelled,
or undecodable output is never displayed as a successful result.

The `PANGROVA_COORDINATOR_URL` environment variable (for example
`http://host:8080`) is parsed into `--host`/`--port` when those options are not
given explicitly.

### Interactive Mode

```
$ bin/pangrova-cli.sh --host localhost --port 8080
Pangrova SQL Shell. Type \q or exit to quit.

pangrova> SELECT 1 + 1 AS result;
+--------+
| result |
+--------+
|      2 |
+--------+

pangrova> SHOW CATALOGS;
```

Type `\q` or `exit` to quit. Statements that produce no result set print
`OK (no result set)`.

### Script Mode

Pipe SQL from a file:

```bash
bin/pangrova-cli.sh --host localhost --port 8080 < queries.sql
```

Or use shell redirection:

```bash
echo "SELECT count(*) FROM my_table;" | bin/pangrova-cli.sh --host localhost --port 8080
```
