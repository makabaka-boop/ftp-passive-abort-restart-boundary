import asyncio
import contextlib
import re
import socket
import unittest

from server import FILES, SessionHooks, start_server


class Gate:
    def __init__(self):
        self.entered = asyncio.Event()
        self.released = asyncio.Event()

    async def wait(self):
        self.entered.set()
        await self.released.wait()

    def release(self):
        self.released.set()


class SlowWriteHooks(SessionHooks):
    def __init__(self):
        self.gate = Gate()

    async def after_write(self, transfer, cursor, chunk):
        if cursor == 0:
            await self.gate.wait()


class CompletionRaceHooks(SessionHooks):
    """Synchronize the two instructions that race for the terminal state."""

    def __init__(self):
        self.finish_reached = asyncio.Event()
        self.finish_allowed = asyncio.Event()
        self.abort_at_decision = asyncio.Event()
        self.abort_allowed = asyncio.Event()

    async def before_finish(self, transfer):
        self.finish_reached.set()
        await self.finish_allowed.wait()

    async def before_abort_state(self, transfer):
        self.abort_at_decision.set()
        await self.abort_allowed.wait()


class ForcedSendFailureHooks(SessionHooks):
    def __init__(self):
        self.fired = False

    async def after_write(self, transfer, cursor, chunk):
        if not self.fired and cursor == 0:
            self.fired = True
            raise ConnectionResetError("simulated data failure")


class FtpControl:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.replies = asyncio.Queue()
        self.reader_task = asyncio.create_task(self._read_replies())

    async def _read_replies(self):
        while True:
            line = await self.reader.readline()
            if not line:
                await self.replies.put(("closed", b""))
                return
            match = re.fullmatch(rb"(\d{3}) (.*)\r\n", line)
            if not match:
                raise AssertionError(f"malformed reply: {line!r}")
            await self.replies.put((int(match.group(1)), line))

    async def send(self, text, delay=0):
        data = text.encode("ascii")
        if delay:
            mid = len(data) // 2
            self.writer.write(data[:mid])
            await self.writer.drain()
            await asyncio.sleep(delay)
            self.writer.write(data[mid:])
        else:
            self.writer.write(data)
        await self.writer.drain()

    async def code(self, expected=None, timeout=5):
        code, line = await asyncio.wait_for(self.replies.get(), timeout)
        if expected is not None:
            assert code == expected, f"expected {expected}, got {line!r}"
        return code

    async def close(self):
        self.writer.close()
        self.reader_task.cancel()
        with contextlib.suppress(Exception):
            await self.writer.wait_closed()
        with contextlib.suppress(asyncio.CancelledError):
            await self.reader_task


async def connect_control(port):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    control = FtpControl(reader, writer)
    await control.code(220)
    return control


async def login(control, split_commands=False):
    delay = 0.02 if split_commands else 0
    await control.send("USER demo\r\n", delay=delay)
    await control.code(331)
    await control.send("PASS demo-pass\r\n", delay=delay)
    await control.code(230)


async def epsv(control):
    await control.send("EPSV\r\n")
    code, line = await asyncio.wait_for(control.replies.get(), 5)
    assert code == 229, line
    match = re.search(rb"\(\|\|\|(\d+)\|\)", line)
    assert match, line
    return int(match.group(1))


def data_socket(receive_buffer=None):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if receive_buffer is not None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
    sock.setblocking(False)
    return sock


async def connect_data(port, receive_buffer=None):
    sock = data_socket(receive_buffer)
    try:
        await asyncio.get_running_loop().sock_connect(
            sock, ("127.0.0.1", port)
        )
    except Exception:
        sock.close()
        raise
    return sock


async def recv_all(sock):
    chunks = []
    while True:
        chunk = await asyncio.get_running_loop().sock_recv(sock, 65536)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


class FtpServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=0.25,
        )
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()

    async def start_session(self):
        control = await connect_control(self.port)
        await login(control, split_commands=True)
        await control.send("TYPE I\r\n")
        await control.code(200)
        return control

    async def download(self, name, restart=None):
        control = await self.start_session()
        port = await epsv(control)
        if restart is not None:
            await control.send(f"REST {restart}\r\n")
            await control.code(350)
        data_sock = await connect_data(port)
        await control.send(f"RETR {name}\r\n")
        await control.code(150)
        data = await recv_all(data_sock)
        await control.code(226)
        data_sock.close()
        await control.close()
        return data

    async def test_login_incremental_crlf_and_full_downloads(self):
        control = await self.start_session()

        # CRLF and a command can be split across TCP segments.
        await control.send("NO", delay=0.01)
        await control.send("OP\r\n", delay=0.01)
        await control.code(200)

        for name, content in FILES.items():
            port = await epsv(control)
            data_sock = await connect_data(port)
            await control.send(f"RETR {name}\r\n")
            await control.code(150)
            assert await recv_all(data_sock) == content
            await control.code(226)
            data_sock.close()

        await control.close()

    async def test_rest_zero_and_offset_are_consumed_once(self):
        assert await self.download("colors.bin", restart=0) == FILES["colors.bin"]
        expected = FILES["counter.bin"][1000:]
        assert await self.download("counter.bin", restart=1000) == expected

        control = await self.start_session()
        await control.send("REST 10\r\n")
        await control.code(350)
        await control.send("RETR missing.bin\r\n")
        await control.code(550)
        # Rejected RETR did not consume REST; but there is no listener yet.
        await control.send("RETR colors.bin\r\n")
        await control.code(425)
        port = await epsv(control)
        data_sock = await connect_data(port)
        await control.send("RETR colors.bin\r\n")
        await control.code(150)
        assert await recv_all(data_sock) == FILES["colors.bin"][10:]
        await control.code(226)
        data_sock.close()
        await control.close()

    async def test_data_accept_timeout_is_425_and_later_abor_is_225(self):
        control = await self.start_session()
        port = await epsv(control)
        await control.send("RETR colors.bin\r\n")
        await control.code(150)
        await control.code(425)
        await control.send("ABOR\r\n")
        await control.code(225)
        assert port > 0
        await control.close()

    async def test_transfer_failure_is_426_then_session_remains_usable(self):
        self.server.close()
        await self.server.wait_closed()
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=0.25,
            hooks=ForcedSendFailureHooks(),
        )
        self.port = self.server.sockets[0].getsockname()[1]

        control = await self.start_session()
        port = await epsv(control)
        data_sock = await connect_data(port)
        await control.send("RETR patterns.bin\r\n")
        await control.code(150)
        await control.code(426)
        with contextlib.suppress(OSError):
            await asyncio.get_running_loop().sock_recv(data_sock, 4096)
        data_sock.close()

        # A new transfer gets a new listener and a completely fresh terminal.
        port = await epsv(control)
        data_sock = await connect_data(port)
        await control.send("RETR colors.bin\r\n")
        await control.code(150)
        assert await recv_all(data_sock) == FILES["colors.bin"]
        await control.code(226)
        data_sock.close()
        await control.close()

    async def test_abor_during_slow_data_send_does_not_share_conclusion(self):
        self.server.close()
        await self.server.wait_closed()
        hooks = SlowWriteHooks()
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=1,
            hooks=hooks,
        )
        self.port = self.server.sockets[0].getsockname()[1]
        control = await self.start_session()

        port = await epsv(control)
        data_sock = await connect_data(port, receive_buffer=256)
        await control.send("RETR patterns.bin\r\n")
        await control.code(150)
        await hooks.gate.entered.wait()

        # The data sender is blocked; the independent control loop still
        # enforces gating and processes ABOR.
        await control.send("TYPE I\r\n")
        await control.code(503)
        await control.send("ABOR\r\n")
        await control.code(426)
        await control.code(226)
        hooks.gate.release()

        # Drain/reset of the aborted socket must not produce another reply.
        await asyncio.sleep(0.05)
        assert control.replies.empty()
        with contextlib.suppress(OSError):
            while await asyncio.get_running_loop().sock_recv(data_sock, 4096):
                pass
        data_sock.close()

        # The old transfer consumed its listener and restart marker.
        await control.send("RETR patterns.bin\r\n")
        await control.code(425)
        port = await epsv(control)
        new_data = await connect_data(port)
        await control.send("RETR colors.bin\r\n")
        await control.code(150)
        assert await recv_all(new_data) == FILES["colors.bin"]
        await control.code(226)
        new_data.close()
        await control.close()

    async def test_completion_and_abor_race_success_wins(self):
        await self.race_test(success_wins=True)

    async def test_completion_and_abor_race_abort_wins(self):
        await self.race_test(success_wins=False)

    async def race_test(self, success_wins: bool):
        self.server.close()
        await self.server.wait_closed()
        hooks = CompletionRaceHooks()
        self.server = await start_server(
            host="127.0.0.1",
            port=0,
            passive_ports=(0, 0),
            accept_timeout=1,
            hooks=hooks,
        )
        self.port = self.server.sockets[0].getsockname()[1]
        control = await self.start_session()
        port = await epsv(control)
        data_sock = await connect_data(port)
        await control.send("RETR patterns.bin\r\n")
        await control.code(150)

        async def read_until_close():
            with contextlib.suppress(OSError):
                while True:
                    chunk = await asyncio.get_running_loop().sock_recv(
                        data_sock, 4096
                    )
                    if not chunk:
                        return

        reader_task = asyncio.create_task(read_until_close())

        await hooks.finish_reached.wait()
        await control.send("ABOR\r\n")
        await hooks.abort_at_decision.wait()

        if success_wins:
            hooks.finish_allowed.set()
            await control.code(226)
            hooks.abort_allowed.set()
            await control.code(225)
        else:
            hooks.abort_allowed.set()
            # Give ABOR its state-claim opportunity while completion remains
            # parked immediately before its own claim.
            await asyncio.sleep(0.02)
            hooks.finish_allowed.set()
            await control.code(426)
            await control.code(226)

        await asyncio.wait_for(reader_task, timeout=2)
        data_sock.close()
        await asyncio.sleep(0.02)
        assert control.replies.empty()
        await control.close()

    async def test_new_epsv_replaces_old_listener_including_late_connection(self):
        control = await self.start_session()
        first_port = await epsv(control)
        stale_sock = await connect_data(first_port)

        second_port = await epsv(control)
        assert second_port != first_port

        # The stale, accepted connection is closed when EPSV replaces its
        # listener.  Give the server a moment to drain that accept queue.
        await asyncio.sleep(0.05)
        late_sock = data_socket()
        with self.assertRaises(ConnectionRefusedError):
            await asyncio.get_running_loop().sock_connect(
                late_sock, ("127.0.0.1", first_port)
            )
        late_sock.close()
        with contextlib.suppress(OSError):
            await asyncio.get_running_loop().sock_recv(stale_sock, 4096)
        stale_sock.close()

        data_sock = await connect_data(second_port)
        await control.send("RETR colors.bin\r\n")
        await control.code(150)
        assert await recv_all(data_sock) == FILES["colors.bin"]
        await control.code(226)
        data_sock.close()
        await control.close()

    async def test_unsupported_commands_and_auth_boundary(self):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        control = FtpControl(reader, writer)
        await control.code(220)

        for command in ("PWD\r\n", "LIST\r\n", "STOR x\r\n", "DELE x\r\n",
                        "PASV\r\n", "PORT 1,2,3,4,0,1\r\n", "MKD x\r\n"):
            await control.send(command)
            await control.code(530)

        await control.send("USER demo\r\n")
        await control.code(331)
        await control.send("PASS wrong\r\n")
        await control.code(530)

        await login(control)
        await control.send("TYPE A\r\n")
        await control.code(504)
        for command in ("PWD\r\n", "LIST\r\n", "STOR x\r\n", "DELE x\r\n",
                        "PASV\r\n", "PORT 1,2,3,4,0,1\r\n", "MKD x\r\n"):
            await control.send(command)
            await control.code(502)
        await control.close()

    async def test_abor_without_transfer_is_225(self):
        control = await self.start_session()
        await control.send("ABOR\r\n")
        await control.code(225)
        await control.close()


if __name__ == "__main__":
    unittest.main()
