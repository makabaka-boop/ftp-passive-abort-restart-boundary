#!/usr/bin/env python3
"""Small, read-only FTP demo server.

The implementation deliberately keeps the control-connection command loop and
each RETR data transfer in separate tasks.  A transfer owns its passive
listener, accepted socket, file cursor, terminal state, and terminal reply;
those objects are never reused by a later transfer.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import re
import socket
from dataclasses import dataclass, field
from typing import Optional, Tuple

logger = logging.getLogger("readonly-ftp")

CRLF = b"\r\n"
MAX_COMMAND_LENGTH = 512

USERNAME = "demo"
PASSWORD = "demo-pass"

# Three fixed in-memory binary demonstration files.  The largest is exactly
# 32 KiB; nothing is read from a mutable upload directory.
def _demo_files() -> dict[str, bytes]:
    ascending = bytes(range(256))

    random_pattern = bytearray(32 * 1024)
    state = 0x12345678
    for index in range(len(random_pattern)):
        state = (1103515245 * state + 12345) & 0x7FFFFFFF
        random_pattern[index] = state & 0xFF

    return {
        "colors.bin": ascending * 16,                 # 4,096 bytes
        "counter.bin": ascending * 64,               # 16,384 bytes
        "patterns.bin": bytes(random_pattern),       # 32,768 bytes
    }


FILES = _demo_files()


class SessionHooks:
    """Integration hooks.  Production uses the no-op implementation."""

    async def after_write(self, transfer: "Transfer", cursor: int, chunk: bytes) -> None:
        pass

    async def before_finish(self, transfer: "Transfer") -> None:
        pass

    async def before_abort_state(self, transfer: "Transfer") -> None:
        pass


NULL_HOOKS = SessionHooks()

_BAD_COMMAND = object()

STATE_ACCEPTING = "accepting"
STATE_ACTIVE = "active"
STATE_SUCCEEDED = "succeeded"
STATE_ABORTED = "aborted"
STATE_FAILED = "failed"
STATE_TIMED_OUT = "timed-out"
STATE_ACCEPT_FAILED = "accept-failed"
STATE_CANCELED = "canceled"
STATE_CLOSING = "closing"

TERMINAL_STATES = {
    STATE_SUCCEEDED,
    STATE_ABORTED,
    STATE_FAILED,
    STATE_TIMED_OUT,
    STATE_ACCEPT_FAILED,
    STATE_CANCELED,
    STATE_CLOSING,
}


@dataclass
class PassiveListener:
    sock: socket.socket
    port: int
    generation: int
    _closed: bool = False

    def close(self) -> None:
        """Close a listener and remove anything already in its accept queue.

        A connection which completed the TCP handshake but has not yet been
        accepted by RETR must not survive to be mistaken for a future
        listener's connection.
        """
        if self._closed:
            return
        self._closed = True
        self.sock.setblocking(False)
        while True:
            try:
                stale, _ = self.sock.accept()
            except BlockingIOError:
                break
            except OSError:
                break
            else:
                with contextlib.suppress(OSError):
                    stale.close()
        with contextlib.suppress(OSError):
            self.sock.close()


@dataclass
class Transfer:
    session: "FtpSession"
    generation: int
    filename: str
    data: bytes
    start_offset: int
    listener: PassiveListener
    hooks: SessionHooks
    accept_timeout: float

    state: str = STATE_ACCEPTING
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    terminal_reply_done: asyncio.Event = field(default_factory=asyncio.Event)
    task: Optional[asyncio.Task[None]] = None
    data_sock: Optional[socket.socket] = None
    data_writer: Optional[asyncio.StreamWriter] = None

    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def close_data_and_listener(self) -> None:
        if self.data_writer is not None:
            transport = self.data_writer.transport
            if transport is not None:
                with contextlib.suppress(Exception):
                    transport.abort()
        elif self.data_sock is not None:
            with contextlib.suppress(OSError):
                self.data_sock.close()
        self.listener.close()

    async def _conclude(self, state: str, code: int, message: str) -> None:
        async with self.state_lock:
            if self.state not in (STATE_ACCEPTING, STATE_ACTIVE):
                return
            self.state = state
        await self.session.transfer_reply(self, code, message)
        self.terminal_reply_done.set()

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            try:
                self.data_sock, _ = await asyncio.wait_for(
                    loop.sock_accept(self.listener.sock),
                    self.accept_timeout,
                )
            except asyncio.TimeoutError:
                await self._conclude(
                    STATE_TIMED_OUT,
                    425,
                    "Can't open data connection.",
                )
                return
            except OSError:
                await self._conclude(
                    STATE_ACCEPT_FAILED,
                    425,
                    "Can't open data connection.",
                )
                return

            # This transfer owns this socket and must not perform any more
            # accepts on it.
            self.listener.close()

            try:
                _, self.data_writer = await asyncio.open_connection(
                    sock=self.data_sock
                )
            except OSError:
                await self._conclude(
                    STATE_ACCEPT_FAILED,
                    425,
                    "Can't open data connection.",
                )
                return

            async with self.state_lock:
                if self.state != STATE_ACCEPTING:
                    return
                self.state = STATE_ACTIVE

            writer = self.data_writer
            view = memoryview(self.data)[self.start_offset:]
            chunk_size = 8192

            for cursor in range(0, len(view), chunk_size):
                chunk = view[cursor:cursor + chunk_size]
                writer.write(chunk)
                await self.hooks.after_write(
                    self, self.start_offset + cursor, bytes(chunk)
                )
                await writer.drain()

            await self.hooks.before_finish(self)

            async with self.state_lock:
                if self.state != STATE_ACTIVE:
                    return
                self.state = STATE_SUCCEEDED

            # Send the data-connection EOF before the successful control reply.
            with contextlib.suppress(Exception):
                writer.close()
                await writer.wait_closed()

            await self.session.transfer_reply(self, 226, "Transfer complete.")
            self.terminal_reply_done.set()

        except asyncio.CancelledError:
            async with self.state_lock:
                if self.state in (STATE_ACCEPTING, STATE_ACTIVE):
                    self.state = STATE_CANCELED
            # ABOR has already emitted its ordered replies; QUIT emits none.
            return
        except (OSError, EOFError, RuntimeError):
            await self._conclude(
                STATE_FAILED,
                426,
                "Connection closed; transfer aborted.",
            )
        except Exception:
            logger.exception("Unhandled transfer error")
            await self._conclude(
                STATE_FAILED,
                426,
                "Connection closed; transfer aborted.",
            )
        finally:
            self.close_data_and_listener()
            async with self.session.state_lock:
                if self.session.active_transfer is self:
                    self.session.active_transfer = None


class FtpSession:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        passive_ports: Tuple[int, int],
        accept_timeout: float,
        hooks: SessionHooks,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.passive_ports = passive_ports
        self.accept_timeout = accept_timeout
        self.hooks = hooks

        self.state_lock = asyncio.Lock()
        self.reply_lock = asyncio.Lock()

        self.authenticated = False
        self.binary_type = False
        self.username: Optional[str] = None
        self.pending_restart: Optional[int] = None
        self.pending_listener: Optional[PassiveListener] = None
        self.active_transfer: Optional[Transfer] = None

        self.closing = False
        self.quit_requested = False
        self.listener_generation = 0

        self.control_buffer = b""
        self.discard_until_crlf = False

    async def reply(self, code: int, message: str) -> bool:
        async with self.reply_lock:
            if self.closing:
                return False
            try:
                self.writer.write(f"{code} {message}\r\n".encode("ascii"))
                await self.writer.drain()
                return True
            except (ConnectionError, OSError, RuntimeError):
                self.closing = True
                return False

    async def transfer_reply(
        self, transfer: Transfer, code: int, message: str
    ) -> None:
        # A stale task belonging to an older transfer can never write a
        # terminal reply for the session's current transfer.
        async with self.state_lock:
            if self.active_transfer is not transfer or not transfer.is_terminal():
                return
        await self.reply(code, message)

    async def _read_command(self):
        """Incrementally scan the control stream for CRLF-delimited commands."""
        while True:
            if self.discard_until_crlf:
                index = self.control_buffer.find(CRLF)
                if index >= 0:
                    self.control_buffer = self.control_buffer[index + 2:]
                    self.discard_until_crlf = False
                else:
                    chunk = await self.reader.read(4096)
                    if not chunk:
                        return None
                    self.control_buffer += chunk
                    continue

            index = self.control_buffer.find(CRLF)
            if index >= 0:
                line = self.control_buffer[:index]
                self.control_buffer = self.control_buffer[index + 2:]
                if index > MAX_COMMAND_LENGTH or b"\n" in line:
                    return _BAD_COMMAND
                return line

            if len(self.control_buffer) > MAX_COMMAND_LENGTH:
                self.discard_until_crlf = True

            chunk = await self.reader.read(4096)
            if not chunk:
                return None
            self.control_buffer += chunk

    async def run(self) -> None:
        try:
            await self.reply(
                220, "Read-only FTP demo service ready (use demo/demo-pass)."
            )
            while not self.closing and not self.quit_requested:
                line = await self._read_command()
                if line is None:
                    break
                if line is _BAD_COMMAND:
                    await self.reply(501, "Syntax error.")
                    continue
                try:
                    text = line.decode("ascii")
                except UnicodeDecodeError:
                    await self.reply(501, "Syntax error.")
                    continue
                await self.dispatch(text)
        except (ConnectionError, OSError, asyncio.CancelledError):
            pass
        finally:
            await self.close()
            with contextlib.suppress(Exception):
                await self.writer.wait_closed()

    async def dispatch(self, text: str) -> None:
        parts = text.split(None, 1)
        verb = parts[0].upper() if parts else ""
        argument = parts[1] if len(parts) == 2 else ""

        if verb not in ("USER", "PASS", "QUIT") and not self.authenticated:
            await self.reply(530, "Login required.")
            return

        async with self.state_lock:
            active = self.active_transfer

        if active is not None and verb not in ("ABOR", "NOOP", "QUIT"):
            await self.reply(503, "Active transfer in progress.")
            return

        method = {
            "USER": self.cmd_user,
            "PASS": self.cmd_pass,
            "TYPE": self.cmd_type,
            "EPSV": self.cmd_epsv,
            "REST": self.cmd_rest,
            "RETR": self.cmd_retr,
            "ABOR": self.cmd_abor,
            "NOOP": self.cmd_noop,
            "QUIT": self.cmd_quit,
        }.get(verb)

        if method is None:
            await self.reply(502, "Command not implemented.")
            return

        await method(argument)

    async def cmd_user(self, argument: str) -> None:
        if not argument or any(ch.isspace() for ch in argument):
            await self.reply(501, "Syntax error.")
            return
        self.username = argument
        self.authenticated = False
        await self.reply(331, "Please specify the password.")

    async def cmd_pass(self, argument: str) -> None:
        if self.authenticated:
            await self.reply(503, "Already logged in.")
            return
        if self.username is None:
            await self.reply(503, "Login with USER first.")
            return
        if self.username == USERNAME and argument == PASSWORD:
            self.authenticated = True
            await self.reply(230, "Login successful.")
        else:
            self.username = None
            await self.reply(530, "Invalid username or password.")

    async def cmd_type(self, argument: str) -> None:
        if argument == "I":
            self.binary_type = True
            await self.reply(200, "Type set to I.")
        else:
            await self.reply(504, "Only TYPE I is supported.")

    def _make_listener(self) -> PassiveListener:
        start, end = self.passive_ports
        candidates = range(start, end + 1) if start else range(1)
        last_error: Optional[OSError] = None

        for port in candidates:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                server_socket.setblocking(False)
                server_socket.setsockopt(
                    socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
                )
                server_socket.bind(("0.0.0.0", port))
                server_socket.listen(1)
            except OSError as exc:
                server_socket.close()
                last_error = exc
                continue

            self.listener_generation += 1
            actual_port = server_socket.getsockname()[1]
            return PassiveListener(
                sock=server_socket,
                port=actual_port,
                generation=self.listener_generation,
            )

        raise OSError("No passive port available") from last_error

    async def cmd_epsv(self, argument: str) -> None:
        if argument:
            await self.reply(501, "EPSV takes no argument in this server.")
            return

        try:
            new_listener = self._make_listener()
        except OSError:
            await self.reply(425, "Can't open passive connection.")
            return

        async with self.state_lock:
            old_listener = self.pending_listener
            self.pending_listener = new_listener
        if old_listener is not None:
            old_listener.close()

        await self.reply(
            229,
            f"Entering Extended Passive Mode (|||{new_listener.port}|).",
        )

    async def cmd_rest(self, argument: str) -> None:
        if not re.fullmatch(r"[0-9]+", argument):
            await self.reply(501, "REST requires a non-negative byte offset.")
            return
        self.pending_restart = int(argument, 10)
        await self.reply(350, f"Restart position accepted ({argument}).")

    async def cmd_retr(self, argument: str) -> None:
        if not argument or any(ch.isspace() for ch in argument):
            await self.reply(501, "RETR requires a file name.")
            return
        if not self.binary_type:
            await self.reply(504, "Use TYPE I before RETR.")
            return

        data = FILES.get(argument)
        if data is None:
            # A rejected RETR does not consume REST or the passive listener.
            await self.reply(550, "File not found.")
            return

        offset = self.pending_restart or 0
        if offset > len(data):
            await self.reply(554, "REST offset is beyond end of file.")
            return

        async with self.state_lock:
            if self.active_transfer is not None:
                await self.reply(503, "Active transfer in progress.")
                return
            listener = self.pending_listener
            if listener is None:
                await self.reply(425, "Use EPSV before RETR.")
                return

            # This accepted RETR exclusively consumes both restart state and
            # the current passive listener.
            self.pending_listener = None
            self.pending_restart = None
            transfer = Transfer(
                session=self,
                generation=listener.generation,
                filename=argument,
                data=data,
                start_offset=offset,
                listener=listener,
                hooks=self.hooks,
                accept_timeout=self.accept_timeout,
            )
            self.active_transfer = transfer

        # Never hold the session state lock while writing on the control
        # socket: a blocked drain here would otherwise freeze ABOR.
        await self.reply(
            150,
            f"Opening BINARY mode data connection for {argument} "
            f"({len(data) - offset} bytes).",
        )
        transfer.task = asyncio.create_task(transfer.run())

    async def cmd_abor(self, argument: str) -> None:
        if argument:
            await self.reply(501, "ABOR takes no argument.")
            return

        async with self.state_lock:
            transfer = self.active_transfer

        if transfer is None:
            await self.reply(225, "No active transfer.")
            return

        if not transfer.is_terminal():
            await self.hooks.before_abort_state(transfer)

        async with transfer.state_lock:
            if transfer.is_terminal():
                already_terminal = True
            else:
                already_terminal = False
                transfer.state = STATE_ABORTED
                transfer.close_data_and_listener()
                if transfer.task is not None:
                    transfer.task.cancel()

        if already_terminal:
            # Its ordered 226/426/425 must be observable before this 225.
            await transfer.terminal_reply_done.wait()
            await self.reply(225, "No active transfer.")
            return

        await self.reply(426, "Connection closed; transfer aborted.")
        await self.reply(226, "ABOR command successful.")
        transfer.terminal_reply_done.set()
        # Do not await the canceled task inline: it may be parked in an
        # independent drain/close call.  Its finally block is all that is
        # left and it cannot write another transfer reply.

    async def cmd_noop(self, argument: str) -> None:
        await self.reply(200, "NOOP successful.")

    async def cmd_quit(self, argument: str) -> None:
        if argument:
            await self.reply(501, "QUIT takes no argument.")
            return
        self.quit_requested = True
        await self.reply(221, "Goodbye.")

    async def close(self) -> None:
        async with self.state_lock:
            if self.closing:
                return
            self.closing = True
            listener = self.pending_listener
            self.pending_listener = None
            transfer = self.active_transfer

        if listener is not None:
            listener.close()

        if transfer is not None and not transfer.is_terminal():
            async with transfer.state_lock:
                if not transfer.is_terminal():
                    transfer.state = STATE_CLOSING
                    transfer.close_data_and_listener()
                    if transfer.task is not None:
                        transfer.task.cancel()

        with contextlib.suppress(Exception):
            self.writer.close()


def parse_port_range(value: str) -> Tuple[int, int]:
    match = re.fullmatch(r"(\d+)(?:-(\d+))?", value)
    if not match:
        raise ValueError("FTP_PASSIVE_PORTS must be 'port' or 'start-end'")
    start = int(match.group(1))
    end = int(match.group(2) or match.group(1))
    if start != 0 and (end < start or start < 1024):
        raise ValueError("invalid passive port range")
    return start, end


async def start_server(
    host: str = "0.0.0.0",
    port: int = 2121,
    passive_ports: Tuple[int, int] = (21100, 21150),
    accept_timeout: float = 10.0,
    hooks: Optional[SessionHooks] = None,
) -> asyncio.base_events.Server:
    hooks = hooks or NULL_HOOKS

    async def client_connected(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        session = FtpSession(
            reader,
            writer,
            passive_ports=passive_ports,
            accept_timeout=accept_timeout,
            hooks=hooks,
        )
        await session.run()

    server = await asyncio.start_server(
        client_connected,
        host,
        port,
        limit=64 * 1024,
    )
    actual_port = server.sockets[0].getsockname()[1] if server.sockets else port
    logger.info(
        "Read-only FTP server listening on %s:%s (passive ports %s)",
        host,
        actual_port,
        f"{passive_ports[0]}-{passive_ports[1]}",
    )
    return server


async def amain() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    host = os.environ.get("FTP_HOST", "0.0.0.0")
    port = int(os.environ.get("FTP_PORT", "2121"))
    passive_ports = parse_port_range(os.environ.get("FTP_PASSIVE_PORTS", "0"))
    accept_timeout = float(os.environ.get("FTP_ACCEPT_TIMEOUT", "10"))

    server = await start_server(
        host=host,
        port=port,
        passive_ports=passive_ports,
        accept_timeout=accept_timeout,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass
