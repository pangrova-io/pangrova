from contextlib import ExitStack
import os
import unittest

from oracle_responses import OracleResponses


class OracleResponsesTest(unittest.TestCase):
    def pipe(self, stack, **limits):
        read_fd, write_fd = os.pipe()
        stack.callback(os.close, write_fd)
        reader = stack.enter_context(os.fdopen(read_fd, "rb", buffering=0))
        responses = stack.enter_context(OracleResponses(reader, **limits))
        return write_fd, responses

    def test_multiple_complete_lines_are_preserved_in_order(self):
        with ExitStack() as stack:
            writer, responses = self.pipe(stack)
            os.write(writer, b'{"status":"BOUND"}\n{"status":"UNBOUND"}\n')
            self.assertEqual('{"status":"BOUND"}\n', responses.next_line())
            self.assertEqual('{"status":"UNBOUND"}\n', responses.next_line())

    def test_exact_byte_bound_includes_newline(self):
        with ExitStack() as stack:
            writer, responses = self.pipe(stack, max_bytes=4)
            os.write(writer, b"abc\n")
            self.assertEqual("abc\n", responses.next_line())
            os.write(writer, b"abcd\n")
            with self.assertRaisesRegex(ValueError, "byte bound"):
                responses.next_line()

    def test_silent_live_writer_expires_without_blocking(self):
        with ExitStack() as stack:
            _, responses = self.pipe(stack, timeout_seconds=0.01)
            with self.assertRaisesRegex(TimeoutError, "deadline"):
                responses.next_line()

    def test_incomplete_eof_and_invalid_utf8_are_not_accepted(self):
        reader_fd, writer_fd = os.pipe()
        with os.fdopen(reader_fd, "rb", buffering=0) as reader, OracleResponses(reader) as responses:
            os.write(writer_fd, b"incomplete")
            os.close(writer_fd)
            with self.assertRaisesRegex(ValueError, "complete response"):
                responses.next_line()
        with ExitStack() as stack:
            writer, responses = self.pipe(stack)
            os.write(writer, b"\xff\n")
            with self.assertRaises(UnicodeDecodeError):
                responses.next_line()

    def test_invalid_limits_fail_before_waiting(self):
        with ExitStack() as stack:
            reader_fd, writer_fd = os.pipe()
            stack.callback(os.close, writer_fd)
            reader = stack.enter_context(os.fdopen(reader_fd, "rb", buffering=0))
            for value in (0, -1, float("nan"), float("inf"), True, 61):
                with self.assertRaises(ValueError):
                    OracleResponses(reader, timeout_seconds=value)
            for value in (0, -1, True, 4194305):
                with self.assertRaises(ValueError):
                    OracleResponses(reader, max_bytes=value)


if __name__ == "__main__":
    unittest.main()
