"""Persistent engine adapters. Each stream owns one client and consumes all results."""

from pathlib import Path
import json
import os
import threading
import selectors
import subprocess
import sys
import tempfile
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request

from tpc_common import ROOT, file_digest, write_json
from tpc_results import typed_result


class QueryFailure(RuntimeError):
    def __init__(self, phase, query_id=None, cancellation='not-requested'):
        super().__init__(phase)
        self.query_id = query_id
        self.cancellation = cancellation


def bearer(config):
    name = config.get('tokenEnv')
    if name is None:
        return None
    if not os.environ.get(name, '').strip():
        raise ValueError('token environment variable must be set and nonblank')
    return os.environ[name]


class PangrovaClient:
    def __init__(self, config, engine):
        jar = Path(config['classpath']).resolve()
        if file_digest(jar) != config['classpathSha256']:
            raise ValueError('persistent client classpath checksum mismatch')
        bearer(config)
        self.temporary = tempfile.TemporaryDirectory(prefix='pangrova-tpc-client-')
        settings = {key: config.get(key, default) for key, default in (
            ('host', 'localhost'), ('port', 8080), ('catalog', 'tpc'), ('schema', 'sf100'),
            ('tls', True), ('allowPlaintextResults', False), ('requestTimeout', 30), ('tokenEnv', None))}
        self.config = {**config, **settings}
        path = Path(self.temporary.name) / 'client.json'
        write_json(path, settings)
        java = str(Path(os.environ['JAVA_HOME']) / 'bin/java') if os.environ.get('JAVA_HOME') else 'java'
        self.errors = tempfile.TemporaryFile()
        try:
            self.process = subprocess.Popen(
                [java, '--add-opens=java.base/java.nio=ALL-UNNAMED', '--source', '17', '-cp', str(jar),
                 str(ROOT / 'scripts/java/TpcClient.java'), str(path)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.errors)
        except OSError:
            self.errors.close()
            self.temporary.cleanup()
            raise
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.buffer = b''
        try:
            ready = self.response(time.monotonic() + 60)
            if ready.get('ready') is not True or ready.get('bridgeVersion') != 1:
                raise ValueError('invalid persistent client startup receipt')
            if not ready.get('javaVersion', '').startswith('17.'):
                raise ValueError('Pangrova benchmark client requires JDK 17')
            self.runtime_info = ready
            if self.request('/v1/server/info').get('server_version') != engine['version']:
                raise ValueError('running Pangrova server version differs from environment manifest')
        except Exception:
            self.close()
            raise

    def response(self, deadline):
        while b'\n' not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self.selector.select(remaining):
                raise QueryFailure('client-process-timeout')
            block = os.read(self.process.stdout.fileno(), 65536)
            if not block:
                raise QueryFailure('client-process-exited')
            self.buffer += block
        line, self.buffer = self.buffer.split(b'\n', 1)
        return json.loads(line)

    def execute(self, sql, timeout):
        query_id = None
        started = time.perf_counter_ns()
        try:
            self.process.stdin.write(json.dumps({'sql': sql, 'timeout': timeout}).encode() + b'\n')
            self.process.stdin.flush()
            deadline = time.monotonic() + timeout + self.config.get('requestTimeout', 30) + 5
            while True:
                outcome = self.response(deadline)
                if outcome.get('event') == 'submitted':
                    query_id = outcome['queryId']
                    continue
                if outcome.get('state') != 'FINISHED':
                    raise QueryFailure(outcome.get('error', 'incomplete-execution') + ':' + outcome.get('errorLocation', ''), outcome.get('queryId'),
                                       outcome.get('cancellation', 'unverified'))
                outcome['adapterElapsedMs'] = outcome['elapsedMs']
                outcome['elapsedMs'] = (time.perf_counter_ns() - started) / 1000000
                outcome['profile'] = self.profile(outcome['queryId'])
                return outcome
        except (OSError, ValueError, QueryFailure) as error:
            if isinstance(error, QueryFailure) and error.query_id:
                raise
            cancellation = 'not-submitted'
            if query_id:
                cancellation = self.cancel(query_id)
            self.close()
            raise QueryFailure(str(error) if isinstance(error, QueryFailure) else type(error).__name__,
                               query_id, cancellation) from error

    def request(self, path, method='GET'):
        scheme = 'https' if self.config.get('tls', True) else 'http'
        url = f"{scheme}://{self.config['host']}:{self.config.get('port', 8080)}" + path
        headers = {'Accept': 'application/json'}
        token = bearer(self.config)
        if token:
            headers['Authorization'] = 'Bearer ' + token
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers, method=method),
                                    timeout=self.config.get('requestTimeout', 30)) as response:
            return json.load(response)

    def cancel(self, query_id):
        try:
            response = self.request('/v1/query/' + urllib.parse.quote(query_id, safe='') + '/cancel', 'POST')
            if response.get('query_id') != query_id:
                return 'invalid-cancellation-identity'
            return response.get('outcome', 'requested')
        except Exception as error:
            return 'request-failed:' + type(error).__name__

    def profile(self, query_id):
        try:
            response = self.request('/v1/query/profile/' + urllib.parse.quote(query_id, safe='') + '/json')
            if response.get('queryId') != query_id or response.get('success') is not True:
                raise QueryFailure('profile-identity-or-status-invalid', query_id)
            # The current public profile tree divides query totals among synthetic
            # stages/tasks; it is not the protocol's measured Native OperatorStats.
            return {'status': 'available', 'scope': 'query-audit-aggregate',
                    'operatorMetricsQualified': False,
                    'derivedFields': ['operatorTree', 'flameGraph', 'timing.ioWaitMs'], 'data': response}
        except urllib.error.HTTPError as error:
            if error.code != 404 or self.config.get('requireProfile'):
                raise QueryFailure('profile-unavailable', query_id) from error
            return {'status': 'unavailable', 'httpStatus': 404}

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.selector.close()
        self.process.stdout.close()
        self.errors.close()
        self.temporary.cleanup()


class TrinoClient:
    def __init__(self, config, engine):
        import trino.dbapi
        import trino.auth
        token = bearer(config)
        self.connection = trino.dbapi.connect(
            host=config['host'], port=config.get('port', 8080), user=config['user'],
            catalog=config['catalog'], schema=config['schema'],
            http_scheme='https' if config.get('tls', True) else 'http',
            auth=trino.auth.JWTAuthentication(token) if token else None,
            session_properties=config.get('sessionProperties', {}),
            max_attempts=1, request_timeout=config.get('requestTimeout', 30))
        cursor = self.connection.cursor()
        try:
            cursor.execute('SELECT version()')
            if cursor.fetchone()[0] != engine['version']:
                raise ValueError('running Trino version differs from environment manifest')
        except Exception:
            self.connection.close()
            raise
        finally:
            cursor.close()
        self.runtime_info = {'pythonVersion': sys.version.split()[0], 'clientVersion': trino.__version__}

    def execute(self, sql, timeout):
        cursor = self.connection.cursor()
        expired = threading.Event()
        def cancel():
            expired.set()
            cursor.cancel()
        timer = threading.Timer(timeout, cancel)
        started = time.perf_counter_ns()
        timer.start()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            description = cursor.description
            result = typed_result([c[0] for c in description], [c[1] for c in description], rows)
            if expired.is_set():
                raise QueryFailure('query-timeout', cursor.query_id, 'requested')
            return {'queryId': cursor.query_id, 'state': 'FINISHED', 'resultConsumed': True,
                    'elapsedMs': (time.perf_counter_ns() - started) / 1000000,
                    'result': result,
                    'serverStats': cursor.stats}
        except Exception as error:
            try:
                cursor.cancel()
                cancellation = 'requested'
            except Exception:
                cancellation = 'request-failed'
            raise QueryFailure('query-timeout' if expired.is_set() else type(error).__name__,
                               cursor.query_id, cancellation) from error
        finally:
            timer.cancel()
            timer.join()
            cursor.close()

    def close(self):
        self.connection.close()


class SparkClient:
    def __init__(self, config, engine):
        from pyspark.sql import SparkSession
        builder = SparkSession.builder.appName('pangrova-tpc-benchmark')
        for key, value in config['sparkConf'].items():
            builder = builder.config(key, value)
        self.spark = builder.getOrCreate().newSession()
        if self.spark.version != engine['version']:
            raise ValueError('running Spark version differs from environment manifest')
        self.spark.catalog.setCurrentCatalog(config['catalog'])
        self.spark.catalog.setCurrentDatabase(config['schema'])
        self.runtime_info = {'pythonVersion': sys.version.split()[0], 'client': 'pyspark'}

    def execute(self, sql, timeout):
        context = self.spark.sparkContext
        group = 'tpc-' + str(uuid.uuid4())
        context.setJobGroup(group, 'TPC query', interruptOnCancel=True)
        expired = threading.Event()
        def cancel():
            expired.set()
            context.cancelJobGroup(group)
        timer = threading.Timer(timeout, cancel)
        started = time.perf_counter_ns()
        timer.start()
        query_id = None
        try:
            frame = self.spark.sql(sql)
            query_id = f'{context.applicationId}/sql:{frame._jdf.queryExecution().id()}'
            rows = frame.collect()
            result = typed_result(frame.columns, [f.dataType.simpleString() for f in frame.schema], rows)
            if expired.is_set():
                raise QueryFailure('query-timeout', query_id, 'requested')
            elapsed = (time.perf_counter_ns() - started) / 1000000
            operators = []
            def visit(node):
                metrics, values = node.metrics().iterator(), {}
                while metrics.hasNext():
                    entry = metrics.next()
                    metric = entry._2()
                    values[str(entry._1())] = {'value': int(metric.value()), 'unit': str(metric.metricType())}
                operators.append({'id': int(node.id()), 'name': str(node.nodeName()), 'metrics': values})
                if str(node.nodeName()) == 'AdaptiveSparkPlan':
                    visit(node.executedPlan())
                else:
                    children = node.children().iterator()
                    while children.hasNext():
                        visit(children.next())
            visit(frame._jdf.queryExecution().executedPlan())
            return {'queryId': query_id, 'state': 'FINISHED', 'resultConsumed': True,
                    'elapsedMs': elapsed,
                    'result': result,
                    'serverStats': {'jobGroup': group, 'operators': operators}}
        except Exception as error:
            context.cancelJobGroup(group)
            raise QueryFailure('query-timeout' if expired.is_set() else type(error).__name__,
                               query_id, 'requested') from error
        finally:
            timer.cancel()
            timer.join()
            context.setLocalProperty('spark.jobGroup.id', None)

    def close(self):
        # Sessions share a SparkContext; only the process owner stops the application.
        self.spark.catalog.clearCache()


def connect(environment):
    factories = {'pangrova': PangrovaClient, 'trino': TrinoClient, 'spark': SparkClient}
    config = {**environment['client'], 'requireProfile': environment.get('settings', {}).get('requireQueryProfile', False)}
    return factories[environment['engine']['name']](config, environment['engine'])
