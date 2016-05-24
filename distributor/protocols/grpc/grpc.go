package grpc

import (
	"net"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	"github.com/coreos/agro"
	"github.com/coreos/agro/distributor/protocols"
	"github.com/coreos/agro/models"
)

const defaultPort = "40000"

func init() {
	protocols.RegisterRPCListener("http", grpcRPCListener)
	protocols.RegisterRPCDialer("http", grpcRPCDialer)
}

func grpcRPCListener(url *url.URL, hdl protocols.RPC, gmd agro.GlobalMetadata) (protocols.RPCServer, error) {
	out := &handler{
		handle: hdl,
	}
	h := url.Host
	if !strings.Contains(h, ":") {
		h = net.JoinHostPort(h, defaultPort)
	}
	lis, err := net.Listen("tcp", h)
	if err != nil {
		return nil, err
	}
	out.grpc = grpc.NewServer()
	models.RegisterAgroStorageServer(out.grpc, out)
	go out.grpc.Serve(lis)
	return out, nil
}

func grpcRPCDialer(url *url.URL, timeout time.Duration, gmd agro.GlobalMetadata) (protocols.RPC, error) {
	h := url.Host
	if !strings.Contains(h, ":") {
		h = net.JoinHostPort(h, defaultPort)
	}
	conn, err := grpc.Dial(h, grpc.WithInsecure(), grpc.WithTimeout(timeout))
	if err != nil {
		return nil, err
	}
	return &client{
		conn:    conn,
		handler: models.NewAgroStorageClient(conn),
	}, nil
}

type client struct {
	conn    *grpc.ClientConn
	handler models.AgroStorageClient
}

func (c *client) Close() error {
	return c.conn.Close()
}

func (c *client) PutBlock(ctx context.Context, ref agro.BlockRef, data []byte) error {
	_, err := c.handler.PutBlock(ctx, &models.PutBlockRequest{
		Refs: []*models.BlockRef{
			ref.ToProto(),
		},
		Blocks: [][]byte{
			data,
		},
	})
	return err
}

func (c *client) Block(ctx context.Context, ref agro.BlockRef) ([]byte, error) {
	resp, err := c.handler.Block(ctx, &models.BlockRequest{
		BlockRef: ref.ToProto(),
	})
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (c *client) RebalanceCheck(ctx context.Context, refs []agro.BlockRef) ([]bool, error) {
	req := &models.RebalanceCheckRequest{}
	for _, x := range refs {
		req.BlockRefs = append(req.BlockRefs, x.ToProto())
	}
	resp, err := c.handler.RebalanceCheck(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Valid, nil
}

func (c *client) WriteBuf(ctx context.Context, ref agro.BlockRef) ([]byte, error) {
	panic("unimplemented")
}

type handler struct {
	handle protocols.RPC
	grpc   *grpc.Server
}

func (h *handler) Block(ctx context.Context, req *models.BlockRequest) (*models.BlockResponse, error) {
	data, err := h.handle.Block(ctx, agro.BlockFromProto(req.BlockRef))
	if err != nil {
		return nil, err
	}
	return &models.BlockResponse{
		Ok:   true,
		Data: data,
	}, nil
}

func (h *handler) PutBlock(ctx context.Context, req *models.PutBlockRequest) (*models.PutResponse, error) {
	for i, ref := range req.Refs {
		err := h.handle.PutBlock(ctx, agro.BlockFromProto(ref), req.Blocks[i])
		if err != nil {
			return nil, err
		}
	}
	return &models.PutResponse{Ok: true}, nil
}

func (h *handler) RebalanceCheck(ctx context.Context, req *models.RebalanceCheckRequest) (*models.RebalanceCheckResponse, error) {
	check := make([]agro.BlockRef, len(req.BlockRefs))
	for i, x := range req.BlockRefs {
		check[i] = agro.BlockFromProto(x)
	}
	out, err := h.handle.RebalanceCheck(ctx, check)
	if err != nil {
		return nil, err
	}
	return &models.RebalanceCheckResponse{
		Valid: out,
	}, nil
}

func (h *handler) Close() error {
	h.grpc.Stop()
	return nil
}
