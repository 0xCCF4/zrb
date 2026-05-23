# ADR 0005 — Version handshake: server sends ServerStatus before ServerHello

## Status
Accepted

## Context
Client and server are deployed independently. A version skew between them (e.g. a protocol-breaking change in a new release) would otherwise surface as a confusing mid-stream I/O error or a JSON parse failure rather than a clear rejection. The compiled crate version (from `Cargo.toml`) is available at build time via `env!("CARGO_PKG_VERSION")` and provides a natural compatibility signal: major and minor version must agree; patch differences are tolerated.

## Decision
Add a `version` field to `ClientHello` and `ServerHello`, populated at compile time. The server validates the client's major and minor version immediately after decoding `ClientHello`. Before sending `ServerHello`, the server always sends a `ServerStatus` — an accept (`ok: true`) or a version-rejection (`ok: false`, human-readable message). The client reads `ServerStatus` first; if rejected it surfaces the message and exits without reading `ServerHello`. If accepted, the existing handshake continues with `ServerHello`.

The full handshake sequence is therefore: `ClientHello` → `ServerStatus` → `ServerHello` → transfer → `ServerStatus`.

## Alternatives considered
- **Stderr-only rejection**: server writes a plain-text error to stderr and closes the connection. SSH pipes stderr back to the client. Rejected: the client cannot distinguish a version rejection from any other SSH/process error; no structured signal.
- **Envelope wrapper**: replace `ServerHello` with a tagged union `{ ok: bool, error?: string, hello?: ServerHello }`. Rejected: requires changing the decoded type on every read path and adds complexity for no additional benefit over a leading `ServerStatus`.
- **Client-side-only check**: include version in `ServerHello`; client rejects after receiving it. Rejected: the server would have already accepted the connection and committed to sending its snapshot list before learning the client is incompatible.

## Consequences
- `ServerStatus` now appears twice on the wire: once as a version gate, once as a transfer result. Both reuse the same type; context (position in the exchange) distinguishes them.
- Old clients (without a `version` field in `ClientHello`) will fail JSON deserialization on the server before a `ServerStatus` can be sent. This is acceptable: there are no deployed instances predating this change.
- The Protocol entry in `CONTEXT.md` is updated to document the new handshake sequence.
