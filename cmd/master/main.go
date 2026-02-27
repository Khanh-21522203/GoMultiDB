package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	rpcpkg "GoMultiDB/internal/rpc"
	"GoMultiDB/internal/server"
	"GoMultiDB/internal/services/ping"
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

	runtime, err := server.NewRuntime(cfg, rpcServer)
	if err != nil {
		log.Fatalf("create runtime: %v", err)
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
