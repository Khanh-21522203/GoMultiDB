package ping

import (
	"context"

	"encoding/json"

	"time"

	dberrors "GoMultiDB/internal/common/errors"

	"GoMultiDB/internal/common/types"

	rpcpkg "GoMultiDB/internal/rpc"
)

type Request struct {
	Message string `json:"message"`
}

type Response struct {
	Message string `json:"message"`

	ServerAt time.Time `json:"server_at"`

	Source string `json:"source"`

	RequestID string `json:"request_id"`
}

type Service struct {
	nodeName string
}

func NewService(nodeName string) *Service {

	return &Service{nodeName: nodeName}

}

func (s *Service) Name() string {

	return "ping"

}

func (s *Service) Methods() map[string]rpcpkg.HandlerFunc {

	return map[string]rpcpkg.HandlerFunc{

		"echo": s.echo,
	}

}

func (s *Service) echo(_ context.Context, env types.RequestEnvelope, payload []byte) ([]byte, error) {

	var req Request

	if err := json.Unmarshal(payload, &req); err != nil {

		return nil, dberrors.New(dberrors.ErrInvalidArgument, "invalid ping request payload", false, err)

	}

	if req.Message == "" {

		req.Message = "pong"

	}

	resp := Response{

		Message: req.Message,

		ServerAt: time.Now().UTC(),

		Source: s.nodeName,

		RequestID: string(env.RequestID),
	}

	b, err := json.Marshal(resp)

	if err != nil {

		return nil, dberrors.New(dberrors.ErrInternal, "marshal ping response", false, err)

	}

	return b, nil

}
