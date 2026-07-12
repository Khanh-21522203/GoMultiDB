// Package heartbeat implements the master's tablet server heartbeat
// protocol: TSManager tracks each registered tablet server's TSDescriptor
// (its instance identity, network registration, and last-seen state), and
// Service handles incoming TSHeartbeat RPCs, applying tablet report deltas
// to a master/catalog.Manager and consulting a
// master/catalog.DirectivePlanner to return corrective TabletActions to
// the reporting tserver. It is consumed by v2/server (the master runtime,
// which registers Service on its infra/rpc.Server) and depends on
// master/catalog for catalog state and contracts/types for the RPC
// envelope.
// This is scaffold-only; behavior is unimplemented.
package heartbeat
