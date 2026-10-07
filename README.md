# Read-Only FTP Demo

A tiny, dependency-free read-only FTP server demonstrating correct concurrency
between the **control connection** and **data connections**. A blocked data
send never blocks the control reader, so `ABOR`, `NOOP`, and `QUIT` remain
usable while a download is in flight.

## Run

```bash
docker compose up --build
```

Connect (any FTP client in binary/passive mode works; account is
`demo` / `demo-pass`):

```bash
python3 - <<'PY'
import ftplib
ftp = ftplib.FTP()
ftp.connect("127.0.0.1", 2121)
ftp.login("demo", "demo-pass")
with open("patterns.bin", "wb") as fh:
    ftp.retrbinary("RETR patterns.bin", fh.write)
ftp.quit()
PY
```

## Fixed demo content

Built into the process — there is no served directory and nothing can be
written:

| File           | Size      |
|----------------|-----------|
| `colors.bin`   | 4,096 B   |
| `counter.bin`  | 16,384 B  |
| `patterns.bin` | 32,768 B (the 32 KiB maximum) |

Account: `demo` / `demo-pass`.

## Supported command set

`USER`, `PASS`, `TYPE I`, `EPSV`, `REST`, `RETR`, `ABOR`, `NOOP`, `QUIT`.

Everything else is rejected with `502`. Upload (`STOR`, `APPE`, ...), active
mode (`PORT`/`EPRT`), and directory operations (`LIST`, `NLST`, `CWD`, `PWD`,
`MKD`, `DELE`, ...) are intentionally unsupported. `TYPE` accepts only `I`.

## Concurrency and protocol rules

* Control commands are read incrementally and framed only on `CRLF`; a command
  split across TCP segments is reassembled. Control reading and data sending
  advance independently (separate asyncio tasks).
* Each `EPSV` opens a fresh passive listener and **replaces** the previous
  one; any connection queued on the old listener but not yet accepted is
  drained and closed, so it can never be mistaken for a later connection.
* Each accepted `RETR` exclusively owns its listener, data socket, file
  cursor, terminal state, and terminal reply. Those objects are never shared
  with a later transfer.
* `REST offset` is consumed only by the **next accepted** `RETR`. A rejected
  `RETR` (missing file, offset past EOF, no listener) does not consume it.
* During an active transfer, every command except `ABOR`, `NOOP`, and `QUIT`
  is explicitly rejected with `503`.
* No data connection accepted within the timeout ⇒ `425`. A data failure while
  sending ⇒ `426`.
* `ABOR`:
  * active transfer: close the data side, then in order `426` then `226`;
  * already finished, or no active transfer: `225`.
* Completion and abort race for a single terminal state under a lock. Exactly
  one conclusion is emitted. A stale `accept`, `drain`, or completion callback
  from an old transfer cannot affect or answer a new one — a terminal reply is
  written only if the transfer is still the session's current one. The client
  can therefore determine which transfer ended solely from control replies.

## Configuration

Environment variables (all optional):

| Variable             | Default          | Meaning                          |
|----------------------|------------------|----------------------------------|
| `FTP_HOST`           | `0.0.0.0`       | Bind address                     |
| `FTP_PORT`           | `2121`           | Control port                     |
| `FTP_PASSIVE_PORTS`  | `0` (ephemeral)  | `port` or `start-end`            |
| `FTP_ACCEPT_TIMEOUT` | `10`             | Seconds to wait for data connect |

## Tests

Real dual-TCP tests with controllable send barriers:

```bash
python3 -m unittest -v
```

They cover abort while the data side is slow-reading, zero-byte `REST` and
offset resume, the end-of-transfer completion race in both orders,
replacement/late arrival of an old listener, `425`/`426` handling, and
rejection of unsupported commands.
