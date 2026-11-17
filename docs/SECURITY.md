# Security notes

## Threat model

`feature-store-lite` relies on SQLite. It is a pure library with no network or I/O beyond the local filesystem.

### Defended

- **Insecure Deserialization**: The library uses explicit type tags in JSON to decode feature values (int, float, bool, str, None). It does not execute arbitrary code.
- **Invalid Input**: All inputs are validated against identifiers regex `[a-z][a-z0-9_]{0,62}` and type checks. Strings are limited to 65k characters for feature values, 1024 for entity IDs.
- **Resource Exhaustion**: `max_rows` prevents SQLite database explosion during ingestion. `too_many_rows` limits entity rows to 100,000 per request.

### Not defended

- **Local Disk Compromise**: If an attacker writes to the SQLite file on disk, they can corrupt the database schema or data. This is a standard risk for local files.
- **Unauthenticated Access**: As a library, this provides no authentication or authorization layer. The calling application is responsible for access control to the SQLite file.
