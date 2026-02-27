package rpc_test

import (
	"context"

	"encoding/json"

	"testing"

	"time"

	dberrors "GoMultiDB/internal/common/errors"

	"GoMultiDB/internal/common/ids"

	"GoMultiDB/internal/common/types"

	rpcpkg "GoMultiDB/internal/rpc"

	"GoMultiDB/internal/services/ping"
)

func TestPhase0_RPCPingAndContractValidation(t *testing.T) {

	ctx := context.Background()

	srv, err := rpcpkg.NewServer(rpcpkg.Config{

		BindAddress: "127.0.0.1:19100",

		StrictContractCheck: true,
	})

	if err != nil {

		t.Fatalf("new server: %v", err)

	}

	if err := srv.RegisterService(ping.NewService("test-node")); err != nil {

		t.Fatalf("register service: %v", err)

	}

	if err := srv.Start(ctx); err != nil {

		t.Fatalf("start server: %v", err)

	}

	defer func() {

		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		defer cancel()

		_ = srv.Stop(stopCtx)

	}()

	client, err := rpcpkg.NewClient(rpcpkg.ClientConfig{

		BaseURL: "http://127.0.0.1:19100",

		Timeout: 5 * time.Second,
	})

	if err != nil {

		t.Fatalf("new client: %v", err)

	}

	reqID := ids.MustNewRequestID()

	env := types.NewRequestEnvelope("client-1", reqID)

	payload, _ := json.Marshal(ping.Request{Message: "hello phase0"})

	respBytes, err := client.Call(ctx, env, "ping", "echo", payload)

	if err != nil {

		t.Fatalf("rpc call failed: %v", err)

	}

	var resp ping.Response

	if err := json.Unmarshal(respBytes, &resp); err != nil {

		t.Fatalf("unmarshal response: %v", err)

	}

	if resp.Message != "hello phase0" {

		t.Fatalf("unexpected message: %q", resp.Message)

	}

	if resp.RequestID != string(reqID) {

		t.Fatalf("unexpected request id echo: got=%s want=%s", resp.RequestID, reqID)

	}

	badEnv := env

	badEnv.ContractVer = 999

	_, err = client.Call(ctx, badEnv, "ping", "echo", payload)

	if err == nil {

		t.Fatalf("expected contract version error")

	}

	var dbErr dberrors.DBError

	if ok := AsDBError(err, &dbErr); !ok {

		t.Fatalf("expected DBError, got %T: %v", err, err)

	}

	if dbErr.Code != dberrors.ErrInvalidArgument {

		t.Fatalf("unexpected error code: %s", dbErr.Code)

	}

}

func AsDBError(err error, target *dberrors.DBError) bool {

	if err == nil {

		return false

	}

	if v, ok := err.(dberrors.DBError); ok {

		*target = v

		return true

	}

	return false

}
