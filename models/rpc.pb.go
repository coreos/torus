// Code generated by protoc-gen-gogo.
// source: rpc.proto
// DO NOT EDIT!

package models

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"
import _ "github.com/gogo/protobuf/gogoproto"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type Block struct {
	Ok   bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	Data []byte `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
}

func (m *Block) Reset()         { *m = Block{} }
func (m *Block) String() string { return proto.CompactTextString(m) }
func (*Block) ProtoMessage()    {}

type BlockRequest struct {
	BlockRefs []*BlockRef `protobuf:"bytes,1,rep,name=block_refs" json:"block_refs,omitempty"`
	Peer      string      `protobuf:"bytes,2,opt,name=peer,proto3" json:"peer,omitempty"`
	Readahead []*BlockRef `protobuf:"bytes,3,rep,name=readahead" json:"readahead,omitempty"`
}

func (m *BlockRequest) Reset()         { *m = BlockRequest{} }
func (m *BlockRequest) String() string { return proto.CompactTextString(m) }
func (*BlockRequest) ProtoMessage()    {}

func (m *BlockRequest) GetBlockRefs() []*BlockRef {
	if m != nil {
		return m.BlockRefs
	}
	return nil
}

func (m *BlockRequest) GetReadahead() []*BlockRef {
	if m != nil {
		return m.Readahead
	}
	return nil
}

type BlockResponse struct {
	Blocks []*Block `protobuf:"bytes,1,rep,name=blocks" json:"blocks,omitempty"`
}

func (m *BlockResponse) Reset()         { *m = BlockResponse{} }
func (m *BlockResponse) String() string { return proto.CompactTextString(m) }
func (*BlockResponse) ProtoMessage()    {}

func (m *BlockResponse) GetBlocks() []*Block {
	if m != nil {
		return m.Blocks
	}
	return nil
}

type PutBlockRequest struct {
	Refs   []*BlockRef `protobuf:"bytes,1,rep,name=refs" json:"refs,omitempty"`
	Blocks [][]byte    `protobuf:"bytes,2,rep,name=blocks" json:"blocks,omitempty"`
}

func (m *PutBlockRequest) Reset()         { *m = PutBlockRequest{} }
func (m *PutBlockRequest) String() string { return proto.CompactTextString(m) }
func (*PutBlockRequest) ProtoMessage()    {}

func (m *PutBlockRequest) GetRefs() []*BlockRef {
	if m != nil {
		return m.Refs
	}
	return nil
}

type PutResponse struct {
	Ok  bool   `protobuf:"varint,1,opt,name=ok,proto3" json:"ok,omitempty"`
	Err string `protobuf:"bytes,2,opt,name=err,proto3" json:"err,omitempty"`
}

func (m *PutResponse) Reset()         { *m = PutResponse{} }
func (m *PutResponse) String() string { return proto.CompactTextString(m) }
func (*PutResponse) ProtoMessage()    {}

type RebalanceCheckRequest struct {
	BlockRefs []*BlockRef `protobuf:"bytes,1,rep,name=block_refs" json:"block_refs,omitempty"`
}

func (m *RebalanceCheckRequest) Reset()         { *m = RebalanceCheckRequest{} }
func (m *RebalanceCheckRequest) String() string { return proto.CompactTextString(m) }
func (*RebalanceCheckRequest) ProtoMessage()    {}

func (m *RebalanceCheckRequest) GetBlockRefs() []*BlockRef {
	if m != nil {
		return m.BlockRefs
	}
	return nil
}

type RebalanceCheckResponse struct {
	Valid  []bool `protobuf:"varint,1,rep,name=valid" json:"valid,omitempty"`
	Status int32  `protobuf:"varint,2,opt,name=status,proto3" json:"status,omitempty"`
}

func (m *RebalanceCheckResponse) Reset()         { *m = RebalanceCheckResponse{} }
func (m *RebalanceCheckResponse) String() string { return proto.CompactTextString(m) }
func (*RebalanceCheckResponse) ProtoMessage()    {}

type EmptyResponse struct {
}

func (m *EmptyResponse) Reset()         { *m = EmptyResponse{} }
func (m *EmptyResponse) String() string { return proto.CompactTextString(m) }
func (*EmptyResponse) ProtoMessage()    {}

func init() {
	proto.RegisterType((*Block)(nil), "models.Block")
	proto.RegisterType((*BlockRequest)(nil), "models.BlockRequest")
	proto.RegisterType((*BlockResponse)(nil), "models.BlockResponse")
	proto.RegisterType((*PutBlockRequest)(nil), "models.PutBlockRequest")
	proto.RegisterType((*PutResponse)(nil), "models.PutResponse")
	proto.RegisterType((*RebalanceCheckRequest)(nil), "models.RebalanceCheckRequest")
	proto.RegisterType((*RebalanceCheckResponse)(nil), "models.RebalanceCheckResponse")
	proto.RegisterType((*EmptyResponse)(nil), "models.EmptyResponse")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// Client API for AgroStorage service

type AgroStorageClient interface {
	Block(ctx context.Context, in *BlockRequest, opts ...grpc.CallOption) (*BlockResponse, error)
	PutBlock(ctx context.Context, in *PutBlockRequest, opts ...grpc.CallOption) (*PutResponse, error)
	RebalanceCheck(ctx context.Context, in *RebalanceCheckRequest, opts ...grpc.CallOption) (*RebalanceCheckResponse, error)
	ReadAheadBlock(ctx context.Context, in *PutBlockRequest, opts ...grpc.CallOption) (*EmptyResponse, error)
}

type agroStorageClient struct {
	cc *grpc.ClientConn
}

func NewAgroStorageClient(cc *grpc.ClientConn) AgroStorageClient {
	return &agroStorageClient{cc}
}

func (c *agroStorageClient) Block(ctx context.Context, in *BlockRequest, opts ...grpc.CallOption) (*BlockResponse, error) {
	out := new(BlockResponse)
	err := grpc.Invoke(ctx, "/models.AgroStorage/Block", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *agroStorageClient) PutBlock(ctx context.Context, in *PutBlockRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := grpc.Invoke(ctx, "/models.AgroStorage/PutBlock", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *agroStorageClient) RebalanceCheck(ctx context.Context, in *RebalanceCheckRequest, opts ...grpc.CallOption) (*RebalanceCheckResponse, error) {
	out := new(RebalanceCheckResponse)
	err := grpc.Invoke(ctx, "/models.AgroStorage/RebalanceCheck", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *agroStorageClient) ReadAheadBlock(ctx context.Context, in *PutBlockRequest, opts ...grpc.CallOption) (*EmptyResponse, error) {
	out := new(EmptyResponse)
	err := grpc.Invoke(ctx, "/models.AgroStorage/ReadAheadBlock", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for AgroStorage service

type AgroStorageServer interface {
	Block(context.Context, *BlockRequest) (*BlockResponse, error)
	PutBlock(context.Context, *PutBlockRequest) (*PutResponse, error)
	RebalanceCheck(context.Context, *RebalanceCheckRequest) (*RebalanceCheckResponse, error)
	ReadAheadBlock(context.Context, *PutBlockRequest) (*EmptyResponse, error)
}

func RegisterAgroStorageServer(s *grpc.Server, srv AgroStorageServer) {
	s.RegisterService(&_AgroStorage_serviceDesc, srv)
}

func _AgroStorage_Block_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(BlockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(AgroStorageServer).Block(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _AgroStorage_PutBlock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(PutBlockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(AgroStorageServer).PutBlock(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _AgroStorage_RebalanceCheck_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(RebalanceCheckRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(AgroStorageServer).RebalanceCheck(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _AgroStorage_ReadAheadBlock_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(PutBlockRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(AgroStorageServer).ReadAheadBlock(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _AgroStorage_serviceDesc = grpc.ServiceDesc{
	ServiceName: "models.AgroStorage",
	HandlerType: (*AgroStorageServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Block",
			Handler:    _AgroStorage_Block_Handler,
		},
		{
			MethodName: "PutBlock",
			Handler:    _AgroStorage_PutBlock_Handler,
		},
		{
			MethodName: "RebalanceCheck",
			Handler:    _AgroStorage_RebalanceCheck_Handler,
		},
		{
			MethodName: "ReadAheadBlock",
			Handler:    _AgroStorage_ReadAheadBlock_Handler,
		},
	},
	Streams: []grpc.StreamDesc{},
}

func (m *Block) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *Block) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Ok {
		data[i] = 0x8
		i++
		if m.Ok {
			data[i] = 1
		} else {
			data[i] = 0
		}
		i++
	}
	if m.Data != nil {
		if len(m.Data) > 0 {
			data[i] = 0x12
			i++
			i = encodeVarintRpc(data, i, uint64(len(m.Data)))
			i += copy(data[i:], m.Data)
		}
	}
	return i, nil
}

func (m *BlockRequest) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *BlockRequest) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.BlockRefs) > 0 {
		for _, msg := range m.BlockRefs {
			data[i] = 0xa
			i++
			i = encodeVarintRpc(data, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(data[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	if len(m.Peer) > 0 {
		data[i] = 0x12
		i++
		i = encodeVarintRpc(data, i, uint64(len(m.Peer)))
		i += copy(data[i:], m.Peer)
	}
	if len(m.Readahead) > 0 {
		for _, msg := range m.Readahead {
			data[i] = 0x1a
			i++
			i = encodeVarintRpc(data, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(data[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func (m *BlockResponse) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *BlockResponse) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Blocks) > 0 {
		for _, msg := range m.Blocks {
			data[i] = 0xa
			i++
			i = encodeVarintRpc(data, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(data[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func (m *PutBlockRequest) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *PutBlockRequest) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Refs) > 0 {
		for _, msg := range m.Refs {
			data[i] = 0xa
			i++
			i = encodeVarintRpc(data, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(data[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	if len(m.Blocks) > 0 {
		for _, b := range m.Blocks {
			data[i] = 0x12
			i++
			i = encodeVarintRpc(data, i, uint64(len(b)))
			i += copy(data[i:], b)
		}
	}
	return i, nil
}

func (m *PutResponse) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *PutResponse) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Ok {
		data[i] = 0x8
		i++
		if m.Ok {
			data[i] = 1
		} else {
			data[i] = 0
		}
		i++
	}
	if len(m.Err) > 0 {
		data[i] = 0x12
		i++
		i = encodeVarintRpc(data, i, uint64(len(m.Err)))
		i += copy(data[i:], m.Err)
	}
	return i, nil
}

func (m *RebalanceCheckRequest) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *RebalanceCheckRequest) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.BlockRefs) > 0 {
		for _, msg := range m.BlockRefs {
			data[i] = 0xa
			i++
			i = encodeVarintRpc(data, i, uint64(msg.Size()))
			n, err := msg.MarshalTo(data[i:])
			if err != nil {
				return 0, err
			}
			i += n
		}
	}
	return i, nil
}

func (m *RebalanceCheckResponse) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *RebalanceCheckResponse) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Valid) > 0 {
		for _, b := range m.Valid {
			data[i] = 0x8
			i++
			if b {
				data[i] = 1
			} else {
				data[i] = 0
			}
			i++
		}
	}
	if m.Status != 0 {
		data[i] = 0x10
		i++
		i = encodeVarintRpc(data, i, uint64(m.Status))
	}
	return i, nil
}

func (m *EmptyResponse) Marshal() (data []byte, err error) {
	size := m.Size()
	data = make([]byte, size)
	n, err := m.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (m *EmptyResponse) MarshalTo(data []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	return i, nil
}

func encodeFixed64Rpc(data []byte, offset int, v uint64) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	data[offset+4] = uint8(v >> 32)
	data[offset+5] = uint8(v >> 40)
	data[offset+6] = uint8(v >> 48)
	data[offset+7] = uint8(v >> 56)
	return offset + 8
}
func encodeFixed32Rpc(data []byte, offset int, v uint32) int {
	data[offset] = uint8(v)
	data[offset+1] = uint8(v >> 8)
	data[offset+2] = uint8(v >> 16)
	data[offset+3] = uint8(v >> 24)
	return offset + 4
}
func encodeVarintRpc(data []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		data[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	data[offset] = uint8(v)
	return offset + 1
}
func (m *Block) Size() (n int) {
	var l int
	_ = l
	if m.Ok {
		n += 2
	}
	if m.Data != nil {
		l = len(m.Data)
		if l > 0 {
			n += 1 + l + sovRpc(uint64(l))
		}
	}
	return n
}

func (m *BlockRequest) Size() (n int) {
	var l int
	_ = l
	if len(m.BlockRefs) > 0 {
		for _, e := range m.BlockRefs {
			l = e.Size()
			n += 1 + l + sovRpc(uint64(l))
		}
	}
	l = len(m.Peer)
	if l > 0 {
		n += 1 + l + sovRpc(uint64(l))
	}
	if len(m.Readahead) > 0 {
		for _, e := range m.Readahead {
			l = e.Size()
			n += 1 + l + sovRpc(uint64(l))
		}
	}
	return n
}

func (m *BlockResponse) Size() (n int) {
	var l int
	_ = l
	if len(m.Blocks) > 0 {
		for _, e := range m.Blocks {
			l = e.Size()
			n += 1 + l + sovRpc(uint64(l))
		}
	}
	return n
}

func (m *PutBlockRequest) Size() (n int) {
	var l int
	_ = l
	if len(m.Refs) > 0 {
		for _, e := range m.Refs {
			l = e.Size()
			n += 1 + l + sovRpc(uint64(l))
		}
	}
	if len(m.Blocks) > 0 {
		for _, b := range m.Blocks {
			l = len(b)
			n += 1 + l + sovRpc(uint64(l))
		}
	}
	return n
}

func (m *PutResponse) Size() (n int) {
	var l int
	_ = l
	if m.Ok {
		n += 2
	}
	l = len(m.Err)
	if l > 0 {
		n += 1 + l + sovRpc(uint64(l))
	}
	return n
}

func (m *RebalanceCheckRequest) Size() (n int) {
	var l int
	_ = l
	if len(m.BlockRefs) > 0 {
		for _, e := range m.BlockRefs {
			l = e.Size()
			n += 1 + l + sovRpc(uint64(l))
		}
	}
	return n
}

func (m *RebalanceCheckResponse) Size() (n int) {
	var l int
	_ = l
	if len(m.Valid) > 0 {
		n += 2 * len(m.Valid)
	}
	if m.Status != 0 {
		n += 1 + sovRpc(uint64(m.Status))
	}
	return n
}

func (m *EmptyResponse) Size() (n int) {
	var l int
	_ = l
	return n
}

func sovRpc(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozRpc(x uint64) (n int) {
	return sovRpc(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Block) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRpc
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: Block: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Block: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Ok", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Ok = bool(v != 0)
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Data = append([]byte{}, data[iNdEx:postIndex]...)
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRpc(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRpc
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *BlockRequest) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRpc
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: BlockRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: BlockRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field BlockRefs", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.BlockRefs = append(m.BlockRefs, &BlockRef{})
			if err := m.BlockRefs[len(m.BlockRefs)-1].Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Peer", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Peer = string(data[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Readahead", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Readahead = append(m.Readahead, &BlockRef{})
			if err := m.Readahead[len(m.Readahead)-1].Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRpc(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRpc
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *BlockResponse) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRpc
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: BlockResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: BlockResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Blocks", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Blocks = append(m.Blocks, &Block{})
			if err := m.Blocks[len(m.Blocks)-1].Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRpc(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRpc
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PutBlockRequest) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRpc
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PutBlockRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PutBlockRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Refs", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Refs = append(m.Refs, &BlockRef{})
			if err := m.Refs[len(m.Refs)-1].Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Blocks", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Blocks = append(m.Blocks, make([]byte, postIndex-iNdEx))
			copy(m.Blocks[len(m.Blocks)-1], data[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRpc(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRpc
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *PutResponse) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRpc
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PutResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PutResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Ok", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Ok = bool(v != 0)
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Err", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + intStringLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Err = string(data[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRpc(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRpc
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *RebalanceCheckRequest) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRpc
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RebalanceCheckRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RebalanceCheckRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field BlockRefs", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthRpc
			}
			postIndex := iNdEx + msglen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.BlockRefs = append(m.BlockRefs, &BlockRef{})
			if err := m.BlockRefs[len(m.BlockRefs)-1].Unmarshal(data[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipRpc(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRpc
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *RebalanceCheckResponse) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRpc
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: RebalanceCheckResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: RebalanceCheckResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Valid", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Valid = append(m.Valid, bool(v != 0))
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Status", wireType)
			}
			m.Status = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				m.Status |= (int32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipRpc(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRpc
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *EmptyResponse) Unmarshal(data []byte) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowRpc
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: EmptyResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: EmptyResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipRpc(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthRpc
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipRpc(data []byte) (n int, err error) {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowRpc
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if data[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowRpc
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthRpc
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowRpc
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := data[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipRpc(data[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthRpc = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowRpc   = fmt.Errorf("proto: integer overflow")
)
