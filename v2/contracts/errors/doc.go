// Package errors declares the shared error type and sentinel errors used
// across GoMultiDB v2. DBError carries a stable, machine-readable code
// alongside a human-readable message and an optional wrapped cause; every
// other v2 module (engine, gateway, server, replication, infra) returns and
// inspects errors through this shape instead of ad hoc error values. It also
// holds ErrNotImplemented, the sentinel every scaffold stub in the v2 module
// tree returns until real behavior is written.
// This is scaffold-only; behavior is unimplemented.
package errors
