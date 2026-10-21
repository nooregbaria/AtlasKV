# Replication protocol

Live writes and startup catch-up both send a `LogEntry` using the binary
codec in [`include/replication_protocol.h`](../include/replication_protocol.h). Successful primary writes return the
exact parsed key, value, operation, and LSN for replication. Catch-up sends
the entries read from the WAL directly. The replica bypasses the text parser.

Each payload uses the existing 4-byte, big-endian message-length frame:

| Payload field | Size | Encoding |
| --- | --- | --- |
| Magic/version | 4 bytes | ASCII `RPL1` |
| Operation | 1 byte | `0x01` PUT, `0x02` REMOVE |
| LSN | 8 bytes | Unsigned, big-endian |
| Key length | 4 bytes | Unsigned, big-endian |
| Value length | 4 bytes | Unsigned, big-endian |
| Key | Key length | Raw bytes |
| Value | Value length | Raw bytes |

Keys are limited to 1 KiB and values to 10 MiB, matching the WAL limits.
REMOVE requires a zero value length. Malformed payloads, unsupported versions,
and trailing bytes receive `INVALID_REPLICATION` without changing the WAL.
Out-of-sequence LSNs receive `LSN_MISMATCH`. Successful writes receive `OK`.
`GET_LSN` and its `LSN <number>` response remain text control messages.
After catch-up, the primary sends `CATCH_UP_COMPLETE`. The replica prints
`Replica catch-up complete. LSN: <number>` using its current WAL LSN, flushes
the output, and acknowledges with `OK`. This also happens when no entries
need to be transferred.

`std::string` is used as a byte buffer; explicit lengths preserve embedded NULs,
non-UTF-8 bytes, empty values, and whitespace. The ordinary client interface
still normalizes whitespace before committing a write. The disk WAL format
is unchanged.

Upgrade and restart both primary and replica together: the previous text
`REPLICATE <lsn> <command>` format is no longer accepted.

Catch-up still assumes the replica WAL is an identical prefix of the primary
WAL. LSN comparison does not detect divergent histories. The replica listens
on loopback without authentication and assumes local processes are trusted.
