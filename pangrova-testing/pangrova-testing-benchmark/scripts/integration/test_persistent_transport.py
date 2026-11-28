#!/usr/bin/env python3
"""Production client against local HTTP + real Flight transport fixtures; no engine calibration."""

import argparse
from datetime import date
from decimal import Decimal
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import sys
import threading
import time
import unittest
from urllib.parse import parse_qs, urlsplit
import uuid

import pyarrow as pa
import pyarrow.flight as flight

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tpc_common import file_digest
from tpc_clients import PangrovaClient, QueryFailure


class TransportTest(unittest.TestCase):
    classpath = None

    @classmethod
    def setUpClass(cls):
        cls.schema = pa.schema([('amount', pa.decimal128(15, 2)), ('day', pa.date32()),
                                ('text', pa.string()), ('nullable', pa.int64()), ('ratio', pa.float64())])
        cls.table = pa.Table.from_pylist([{'amount': Decimal('123456789.12'), 'day': date(2001, 1, 2),
                                         'text': '完整结果; ' + 'x' * 200, 'nullable': None, 'ratio': 0.125}], schema=cls.schema)
        cls.queries, cls.cancellations, cls.flight_reads = {}, [], []
        cls.status_targets = {}
        class Server(flight.FlightServerBase):
            def do_get(self, context, ticket):
                query_id = ticket.ticket.decode()
                cls.flight_reads.append(query_id)
                sql = cls.queries[query_id]
                table = cls.table.slice(0, 0) if sql == 'EMPTY' else cls.table
                return flight.RecordBatchStream(table)
        cls.flight = Server(('127.0.0.1', 0))
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args): pass
            def send(self, code, body):
                raw = json.dumps(body).encode()
                self.send_response(code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            def do_POST(self):
                if self.path == '/v1/query/submit':
                    if self.headers.get('Authorization') != 'Bearer transport-fixture-only':
                        self.send(401, {'error_code': 'UNAUTHORIZED', 'error_type': 'AUTH', 'message': 'denied'})
                        return
                    request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                    query_id = str(uuid.uuid4())
                    cls.queries[query_id] = request['query']
                    self.send(200, {'query_id': query_id, 'state': 'RUNNING',
                                    'result_disposition': 'PENDING', 'column_names': []})
                elif self.path.endswith('/cancel'):
                    query_id = self.path.split('/')[3]
                    cls.cancellations.append(query_id)
                    self.send(202, {'query_id': query_id, 'cancelled': True, 'outcome': 'CANCEL_ACCEPTED'})
                else:
                    self.send(404, {})
            def do_GET(self):
                endpoint = urlsplit(self.path)
                if endpoint.path == '/v1/server/info':
                    self.send(200, {'server_version': 'fixture'})
                elif endpoint.path.endswith('/state'):
                    query_id = endpoint.path.split('/')[3]
                    query = parse_qs(endpoint.query)
                    cls.status_targets[query_id] = query.get('until', [None])[0]
                    sql = cls.queries[query_id]
                    state = 'RUNNING' if sql == 'TIMEOUT' else 'FAILED' if sql == 'FAIL' else 'FINISHED'
                    if state == 'RUNNING':
                        time.sleep(int(query.get('waitMs', ['0'])[0]) / 1000)
                    disposition = ('NONE' if sql == 'NO_RESULT' or state == 'FAILED'
                                   else 'STREAM' if state == 'FINISHED' else 'PENDING')
                    body = {'query_id': query_id, 'state': state, 'version': 1, 'drained': False,
                            'result_disposition': disposition}
                    if disposition == 'STREAM':
                        body['result_endpoint'] = {'host': '127.0.0.1', 'port': cls.flight.port,
                                                   'ticket': query_id, 'protocol': 'grpc'}
                    self.send(200, body)
                else:
                    self.send(404, {})
        cls.http = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=cls.http.serve_forever, daemon=True)
        cls.thread.start()
        os.environ['TPC_TRANSPORT_TEST_TOKEN'] = 'transport-fixture-only'

    @classmethod
    def tearDownClass(cls):
        cls.http.shutdown()
        cls.http.server_close()
        cls.thread.join()
        cls.flight.shutdown()
        os.environ.pop('TPC_TRANSPORT_TEST_TOKEN', None)

    def client(self):
        client = PangrovaClient({'classpath': str(self.classpath), 'classpathSha256': file_digest(self.classpath),
                                 'host': '127.0.0.1', 'port': self.http.server_port, 'tls': False,
                                 'allowPlaintextResults': True, 'catalog': 'tpc', 'schema': 'transport_fixture',
                                 'tokenEnv': 'TPC_TRANSPORT_TEST_TOKEN', 'requestTimeout': 3}, {'version': 'fixture'})
        self.addCleanup(client.close)
        return client

    def test_two_queries_reuse_process_consume_flight_and_preserve_full_typed_values(self):
        client = self.client()
        pid = client.process.pid
        first = client.execute("SELECT '; do not split this SQL'", 10)
        second = client.execute('SECOND', 10)
        self.assertEqual(pid, client.process.pid)
        self.assertNotEqual(first['queryId'], second['queryId'])
        self.assertEqual(self.queries[first['queryId']], "SELECT '; do not split this SQL'")
        self.assertEqual(self.status_targets[first['queryId']], 'RESULT_OR_TERMINAL')
        self.assertIn(first['queryId'], self.flight_reads)
        self.assertTrue(first['resultConsumed'])
        self.assertGreater(first['elapsedMs'], 0)
        self.assertEqual(first['result']['rows'][0], [{'decimal': '123456789.12'}, {'date': '2001-01-02'},
                                                       '完整结果; ' + 'x' * 200, None, 0.125])
        self.assertEqual(first['profile']['status'], 'unavailable')

    def test_empty_result_keeps_schema(self):
        outcome = self.client().execute('EMPTY', 10)
        self.assertEqual(outcome['result']['rows'], [])
        self.assertEqual(len(outcome['result']['columns']), 5)

    def test_resultless_completion_does_not_open_a_flight_stream(self):
        outcome = self.client().execute('NO_RESULT', 10)
        self.assertEqual(outcome['state'], 'FINISHED')
        self.assertFalse(outcome['resultConsumed'])
        self.assertNotIn('result', outcome)
        self.assertNotIn(outcome['queryId'], self.flight_reads)

    def test_timeout_and_server_failure_retain_query_identity_and_request_cancel(self):
        client = self.client()
        for sql, timeout, failure in [('TIMEOUT', 1, 'query-timeout'), ('FAIL', 10, 'server-failed')]:
            with self.subTest(sql=sql), self.assertRaises(QueryFailure) as context:
                client.execute(sql, timeout)
            self.assertEqual(str(context.exception).split(':', 1)[0], failure)
            self.assertEqual(self.queries[context.exception.query_id], sql)
            self.assertIn(context.exception.query_id, self.cancellations)
            self.assertEqual(context.exception.cancellation, 'CANCEL_ACCEPTED')

    def test_wrong_credentials_fail_without_a_successful_execution(self):
        os.environ['TPC_TRANSPORT_TEST_TOKEN'] = 'wrong-fixture-token'
        try:
            with self.assertRaises(QueryFailure):
                self.client().execute('AUTH-FAIL', 3)
        finally:
            os.environ['TPC_TRANSPORT_TEST_TOKEN'] = 'transport-fixture-only'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--classpath', type=Path, required=True)
    args = parser.parse_args()
    TransportTest.classpath = args.classpath.resolve()
    unittest.main(argv=[sys.argv[0]], verbosity=2)
