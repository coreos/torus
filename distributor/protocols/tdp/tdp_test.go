package tdp

import (
	"bytes"
	"errors"
	"math/rand"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/coreos/torus"
	"github.com/coreos/torus/models"
	"golang.org/x/net/context"
)

var nchecks = rand.Intn(255)

type mockBlockRPC struct {
	data []byte
}

func (m *mockBlockRPC) Block(ctx context.Context, ref torus.BlockRef) ([]byte, error) {
	return m.data, nil
}
func (m *mockBlockRPC) PutBlock(ctx context.Context, ref torus.BlockRef, data []byte) error {
	if ref.INode != 2 && ref.Index != 3 {
		return errors.New("mismatch")
	}
	if bytes.Equal(m.data, data) {
		return nil
	}
	return errors.New("mismatch")
}
func (m *mockBlockRPC) RebalanceCheck(ctx context.Context, refs []torus.BlockRef) ([]bool, error) {
	out := make([]bool, len(refs))
	for i, x := range refs {
		if x.INode%2 == 1 {
			out[i] = true
		}
	}
	return out, nil
}

func (m *mockBlockRPC) WriteBuf(ctx context.Context, ref torus.BlockRef) ([]byte, error) {
	if ref.INode != 2 && ref.Index != 3 {
		return nil, errors.New("mismatch")
	}
	return m.data, nil
}

func (m *mockBlockRPC) BlockSize() uint64 {
	return uint64(len(m.data))
}

type mockBlockGRPC struct {
	data []byte
}

func (g *mockBlockGRPC) Block(ctx context.Context, req *models.BlockRequest) (*models.BlockResponse, error) {
	return &models.BlockResponse{
		Ok:   true,
		Data: g.data,
	}, nil
}

func (g *mockBlockGRPC) PutBlock(ctx context.Context, req *models.PutBlockRequest) (*models.PutResponse, error) {
	ref := torus.BlockFromProto(req.Refs[0])
	if ref.INode != 2 && ref.Index != 3 {
		return &models.PutResponse{
			Err: "bad ref",
		}, nil
	}
	if bytes.Equal(g.data, req.Blocks[0]) {
		return &models.PutResponse{
			Ok: true,
		}, nil
	}
	return &models.PutResponse{
		Err: "mismatch",
	}, nil
}

func (g *mockBlockGRPC) RebalanceCheck(ctx context.Context, req *models.RebalanceCheckRequest) (*models.RebalanceCheckResponse, error) {
	out := make([]bool, len(req.BlockRefs))
	for i, x := range req.BlockRefs {
		p := torus.BlockFromProto(x)
		if p.INode%2 == 1 {
			out[i] = true
		}
	}
	return &models.RebalanceCheckResponse{
		Valid: out,
	}, nil
}

func makeTestData(size int) []byte {
	out := make([]byte, size)
	_, err := rand.Read(out)
	if err != nil {
		panic(err)
	}
	return out
}

func TestBlock(t *testing.T) {
	test := makeTestData(512 * 1024)
	m := &mockBlockRPC{
		data: test,
	}
	s, err := Serve("localhost:40000", m, m.BlockSize())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, err := Dial("localhost:40000", time.Second, m.BlockSize())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, err := c.Block(context.TODO(), torus.BlockRef{
		INodeRef: torus.NewINodeRef(1, 2),
		Index:    3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(test, b) {
		t.Fatal("unequal response")
	}
}

func TestBlockGRPC(t *testing.T) {
	test := makeTestData(512 * 1024)
	m := &mockBlockGRPC{
		data: test,
	}
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := grpc.NewServer()
	models.RegisterTorusStorageServer(grpcSrv, m)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure(), grpc.WithTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := models.NewTorusStorageClient(conn)
	resp, err := client.Block(context.TODO(), &models.BlockRequest{
		BlockRef: torus.BlockRef{
			INodeRef: torus.NewINodeRef(1, 2),
			Index:    3,
		}.ToProto(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(test, resp.Data) {
		t.Fatal("unequal response")
	}
}

func TestPutBlock(t *testing.T) {
	test := makeTestData(512 * 1024)
	m := &mockBlockRPC{
		data: test,
	}
	s, err := Serve("localhost:40000", m, m.BlockSize())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, err := Dial("localhost:40000", time.Second, m.BlockSize())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = c.PutBlock(context.TODO(), torus.BlockRef{
		INodeRef: torus.NewINodeRef(1, 2),
		Index:    3,
	}, test)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPutBlockGRPC(t *testing.T) {
	test := makeTestData(512 * 1024)
	m := &mockBlockGRPC{
		data: test,
	}
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := grpc.NewServer()
	models.RegisterTorusStorageServer(grpcSrv, m)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure(), grpc.WithTimeout(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := models.NewTorusStorageClient(conn)
	resp, err := client.PutBlock(context.TODO(), &models.PutBlockRequest{
		Refs: []*models.BlockRef{
			torus.BlockRef{
				INodeRef: torus.NewINodeRef(1, 2),
				Index:    3,
			}.ToProto(),
		},
		Blocks: [][]byte{
			test,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Ok {
		t.Fatal("unequal response")
	}
}

func TestRebalanceCheck(t *testing.T) {
	test := make([]torus.BlockRef, nchecks)
	m := &mockBlockRPC{}
	for i, _ := range test {
		test[i].Index = 3
		test[i].INodeRef = torus.NewINodeRef(1, torus.INodeID(rand.Intn(40)))
	}
	s, err := Serve("localhost:40000", m, m.BlockSize())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, err := Dial("localhost:40000", time.Second, m.BlockSize())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	bs, err := c.RebalanceCheck(context.TODO(), test)
	if err != nil {
		t.Fatal(err)
	}
	for i, x := range bs {
		if x {
			if test[i].INode%2 == 0 {
				t.Fatal("unequal")
			}
		} else {
			if test[i].INode%2 == 1 {
				t.Fatal("unequal")
			}
		}
	}
}

// BENCHES

func BenchmarkBlock(b *testing.B) {
	test := makeTestData(512 * 1024)
	m := &mockBlockRPC{
		data: test,
	}
	s, err := Serve("localhost:0", m, m.BlockSize())
	if err != nil {
		b.Fatal(err)
	}
	c, err := Dial(s.ListenAddr().String(), time.Second, m.BlockSize())
	if err != nil {
		b.Fatal(err)
	}
	total := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := c.Block(context.TODO(), torus.BlockRef{
			INodeRef: torus.NewINodeRef(1, 2),
			Index:    3,
		})
		if err != nil {
			b.Fatal(err)
		}
		if !bytes.Equal(test, data) {
			b.Fatal("unequal response")
		}
		total += len(data)
		b.ReportAllocs()
	}
	b.SetBytes(int64(total / b.N))
}

func BenchmarkGRPCBlock(b *testing.B) {
	test := makeTestData(512 * 1024)
	m := &mockBlockGRPC{
		data: test,
	}
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	grpcSrv := grpc.NewServer()
	models.RegisterTorusStorageServer(grpcSrv, m)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure(), grpc.WithTimeout(10*time.Second))
	defer conn.Close()
	if err != nil {
		b.Fatal(err)
	}
	client := models.NewTorusStorageClient(conn)
	total := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Block(context.TODO(), &models.BlockRequest{
			BlockRef: torus.BlockRef{
				INodeRef: torus.NewINodeRef(1, 2),
				Index:    3,
			}.ToProto(),
		})
		if err != nil {
			b.Fatal(err)
		}
		if !bytes.Equal(test, resp.Data) {
			b.Fatal("unequal response")
		}
		total += len(resp.Data)
	}
	b.SetBytes(int64(total / b.N))
}

func BenchmarkPutBlock(b *testing.B) {
	test := makeTestData(512 * 1024)
	m := &mockBlockRPC{
		data: test,
	}
	s, err := Serve("localhost:40000", m, m.BlockSize())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	c, err := Dial("localhost:40000", time.Second, m.BlockSize())
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	total := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = c.PutBlock(context.TODO(), torus.BlockRef{
			INodeRef: torus.NewINodeRef(1, 2),
			Index:    3,
		}, test)
		if err != nil {
			b.Fatal(err)
		}
		total += len(test)
	}
	b.SetBytes(int64(total / b.N))
}

func BenchmarkGRPCPutBlock(b *testing.B) {
	test := makeTestData(512 * 1024)
	m := &mockBlockGRPC{
		data: test,
	}
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	grpcSrv := grpc.NewServer()
	models.RegisterTorusStorageServer(grpcSrv, m)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure(), grpc.WithTimeout(10*time.Second))
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	client := models.NewTorusStorageClient(conn)
	total := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.PutBlock(context.TODO(), &models.PutBlockRequest{
			Refs: []*models.BlockRef{
				torus.BlockRef{
					INodeRef: torus.NewINodeRef(1, 2),
					Index:    3,
				}.ToProto(),
			},
			Blocks: [][]byte{
				test,
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		if !resp.Ok {
			b.Fatal("unequal response")
		}
		total += len(test)
	}
	b.SetBytes(int64(total / b.N))
}

func BenchmarkRebalanceCheck(b *testing.B) {
	test := make([]torus.BlockRef, nchecks)
	m := &mockBlockRPC{}
	for i, _ := range test {
		test[i].Index = 3
		test[i].INodeRef = torus.NewINodeRef(1, torus.INodeID(rand.Intn(40)))
	}
	s, err := Serve("localhost:40000", m, m.BlockSize())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	c, err := Dial("localhost:40000", time.Second, m.BlockSize())
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	total := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bs, err := c.RebalanceCheck(context.TODO(), test)
		if err != nil {
			b.Fatal(err)
		}
		for i, x := range bs {
			if x {
				if test[i].INode%2 == 0 {
					b.Fatal("unequal")
				}
			} else {
				if test[i].INode%2 == 1 {
					b.Fatal("unequal")
				}
			}
		}
		total += len(bs)
	}
	b.SetBytes(int64(total / b.N))
}

func BenchmarkGRPCRebalanceCheck(b *testing.B) {
	test := make([]torus.BlockRef, nchecks)
	m := &mockBlockGRPC{}
	for i, _ := range test {
		test[i].Index = 3
		test[i].INodeRef = torus.NewINodeRef(1, torus.INodeID(rand.Intn(40)))
	}
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		b.Fatal(err)
	}
	grpcSrv := grpc.NewServer()
	models.RegisterTorusStorageServer(grpcSrv, m)
	go grpcSrv.Serve(lis)
	defer grpcSrv.Stop()
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure(), grpc.WithTimeout(10*time.Second))
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	client := models.NewTorusStorageClient(conn)
	total := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		refs := make([]*models.BlockRef, 0, len(test))
		for _, x := range test {
			refs = append(refs, x.ToProto())
		}
		resp, err := client.RebalanceCheck(context.TODO(), &models.RebalanceCheckRequest{
			BlockRefs: refs,
		})
		if err != nil {
			b.Fatal(err)
		}
		for i, x := range resp.Valid {
			if x {
				if test[i].INode%2 == 0 {
					b.Fatal("unequal")
				}
			} else {
				if test[i].INode%2 == 1 {
					b.Fatal("unequal")
				}
			}
		}
		total += len(resp.Valid)
	}
	b.SetBytes(int64(total / b.N))
}
