package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"GoMultiDB/internal/master/catalog"
	"GoMultiDB/internal/master/heartbeat"
	"GoMultiDB/internal/master/registry"
	"GoMultiDB/internal/master/snapshot"
	"GoMultiDB/internal/master/syscatalog"
	rpcpkg "GoMultiDB/internal/rpc"
	"GoMultiDB/internal/server"
	"GoMultiDB/internal/services/ping"
	"GoMultiDB/internal/storage/rocks"
)

func main() {
	var (
		nodeID   = flag.String("node-id", "master-1", "node id")
		rpcAddr  = flag.String("rpc-addr", "127.0.0.1:7100", "rpc bind address")
		httpAddr = flag.String("http-addr", "127.0.0.1:7000", "http bind address")
	)
	flag.Parse()

	cfg := server.DefaultConfig()
	cfg.NodeID = *nodeID
	cfg.RPCBindAddress = *rpcAddr
	cfg.HTTPBindAddress = *httpAddr

	rpcServer, err := rpcpkg.NewServer(rpcpkg.Config{
		BindAddress:         cfg.RPCBindAddress,
		StrictContractCheck: cfg.StrictContractCheck,
	})
	if err != nil {
		log.Fatalf("create rpc server: %v", err)
	}
	if err := rpcServer.RegisterService(ping.NewService(*nodeID)); err != nil {
		log.Fatalf("register ping service: %v", err)
	}

	// Create rocks-backed store for catalog and snapshot persistence.
	rocksStore := rocks.NewMemoryStore()

	// Initialize master catalog manager with syscatalog store.
	sysCatalogStore := syscatalog.NewSysCatalogStore(rocksStore)
	catalogMgr, err := catalog.NewManager(sysCatalogStore)
	if err != nil {
		log.Fatalf("create catalog manager: %v", err)
	}

	// Initialize heartbeat service with tablet tracking.
	tsManager := heartbeat.NewTSManager()
	reconcileSink := catalog.NewMemoryReconcileSink()
	catalogMgr.SetReconcileSink(reconcileSink)
	heartbeatSvc := heartbeat.NewService(tsManager, catalogMgr)

	// Create tablet RPC registry for snapshot operations.
	// Wrap the TSManager and reconcileSink to match registry interfaces.
	tabletRegistry := registry.NewTabletRPCRegistry(
		&tsManagerAdapter{tsManager: tsManager},
		&reconcileSinkAdapter{sink: reconcileSink},
	)

	// Create runtime with tablet registry.
	runtime, err := server.NewRuntimeWithTabletRPC(cfg, rpcServer, rocksStore, tabletRegistry)
	if err != nil {
		log.Fatalf("create runtime: %v", err)
	}

	// Set catalog manager as leader for this master (single-master mode for now).
	catalogMgr.SetLeader(true)
	heartbeatSvc.SetLeader(true)

	// Register heartbeat service on RPC server.
	if err := rpcServer.RegisterService(heartbeatSvc); err != nil {
		log.Fatalf("register heartbeat service: %v", err)
	}

	// Register snapshot service if coordinator is enabled.
	if snapCoord := runtime.GetSnapshotCoordinator(); snapCoord != nil {
		if err := rpcServer.RegisterService(snapshot.NewService(snapCoord)); err != nil {
			log.Fatalf("register snapshot service: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := runtime.Init(ctx); err != nil {
		log.Fatalf("runtime init: %v", err)
	}
	if err := runtime.Start(ctx); err != nil {
		log.Fatalf("runtime start: %v", err)
	}

	log.Printf("master started node=%s rpc=%s", *nodeID, *rpcAddr)
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := runtime.Stop(shutdownCtx); err != nil {
		log.Fatalf("runtime stop: %v", err)
	}
	log.Printf("master stopped node=%s", *nodeID)
}

// tsManagerAdapter adapts heartbeat.TSManager to registry.TSManager interface.
type tsManagerAdapter struct {
	tsManager *heartbeat.TSManager
}

func (a *tsManagerAdapter) Get(uuid string) (registry.TSDescriptor, bool) {
	desc, ok := a.tsManager.Get(uuid)
	if !ok {
		return registry.TSDescriptor{}, false
	}
	return registry.TSDescriptor{
		Instance: registry.TSInstance{
			PermanentUUID: desc.Instance.PermanentUUID,
			InstanceSeqNo: desc.Instance.InstanceSeqNo,
		},
		Registration: registry.TSRegistration{
			RPCAddress:  desc.Registration.RPCAddress,
			HTTPAddress: desc.Registration.HTTPAddress,
		},
		LastHeartbeatAt: desc.LastHeartbeatAt,
	}, true
}

// reconcileSinkAdapter adapts catalog.MemoryReconcileSink to registry.ReconcileSink interface.
type reconcileSinkAdapter struct {
	sink *catalog.MemoryReconcileSink
}

func (a *reconcileSinkAdapter) GetTablet(tabletID string) (registry.TabletPlacementView, bool) {
	view, ok := a.sink.GetTablet(tabletID)
	if !ok {
		return registry.TabletPlacementView{}, false
	}
	replicas := make(map[string]registry.TabletReplicaStatus, len(view.Replicas))
	for k, v := range view.Replicas {
		replicas[k] = registry.TabletReplicaStatus{
			TSUUID:    v.TSUUID,
			LastSeqNo: v.LastSeqNo,
		}
	}
	return registry.TabletPlacementView{
		TabletID:      view.TabletID,
		Replicas:      replicas,
		PrimaryTSUUID: view.PrimaryTSUUID,
		Tombstoned:    view.Tombstoned,
		LastUpdated:   view.LastUpdated,
	}, true
}
