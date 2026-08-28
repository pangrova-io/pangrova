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
