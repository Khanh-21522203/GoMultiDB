// Package remotebootstrap implements source-side session management and
// the destination-side client that drives full tablet bootstrap from a
// remote peer: source-side Manager exposes StartRemoteBootstrap,
// FetchManifest, FetchFileChunk, and FinalizeBootstrap, while
// destination-side Client drives session setup, chunked transfer with
// per-chunk CRC32 verification, and atomic install via an Installer. It is
// consumed by v2/engine/tablet when recovering a tombstoned or failed
// tablet from a healthy peer.
// This is scaffold-only; behavior is unimplemented.
package remotebootstrap
