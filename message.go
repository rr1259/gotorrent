package main

import "encoding/binary"

const (
	CHOKE = iota
	UNCHOKE
	INTERESTED
	NOT_INTERESTED
	HAVE
	BITFIELD
	REQUEST
	PIECE
)

const (
	HANDSHAKE  = 11
	KEEP_ALIVE = 10
	DISCONNECT = 12
	QUIT       = 13
)

type MessageId int

// Manager to Worker Messages
type Message struct {
	Id      MessageId
	Address string
	Data    []byte
}

type RequestMessage struct {
	Idx    uint32
	Begin  uint32
	Length uint32
}

type HaveMessage struct {
	Idx uint32
}

type BitfieldMessage struct {
	Bitfield []byte
}

type PieceMessage struct {
	Idx   uint32
	Begin uint32
	Block []byte
}

func NewControlMessage(address string, id MessageId) *Message {
	return &Message{
		Id:      id,
		Address: address,
		Data:    nil,
	}
}

func (m *Message) GetRequestMessage() (r *RequestMessage) {
	r = &RequestMessage{}
	idx := binary.BigEndian.Uint32(m.Data[:4])
	begin := binary.BigEndian.Uint32(m.Data[4:8])
	length := binary.BigEndian.Uint32(m.Data[8:12])
	r.Idx = idx
	r.Begin = begin
	r.Length = length
	return r
}

func (m *Message) GetPieceMessage() (p *PieceMessage) {
	p = &PieceMessage{}
	idx := binary.BigEndian.Uint32(m.Data[:4])
	begin := binary.BigEndian.Uint32(m.Data[4:8])
	block := m.Data[8:len(m.Data)]
	p.Idx = idx
	p.Begin = begin
	p.Block = block
	return p
}

func (m *Message) GetHaveMessage() (h *HaveMessage) {
	h = &HaveMessage{}
	h.Idx = binary.BigEndian.Uint32(m.Data[:4])
	return h
}

// TODO: fill out
func (m *Message) GetBitfieldMessage() (b *BitfieldMessage) {
	b = &BitfieldMessage{}
	b.Bitfield = m.Data
	return b
}

func NewBitfieldMessage(address string, bitfield []byte) *Message {
	return &Message{
		Id:      BITFIELD,
		Address: address,
		Data:    bitfield,
	}
}

func NewHaveMessage(address string, index uint32) *Message {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, index)

	return &Message{
		Id:      HAVE,
		Address: address,
		Data:    data,
	}
}

func NewRequestMessage(address string, index, begin, length uint32) *Message {
	data := make([]byte, 12)
	binary.BigEndian.PutUint32(data[0:4], index)
	binary.BigEndian.PutUint32(data[4:8], begin)
	binary.BigEndian.PutUint32(data[8:12], length)

	return &Message{
		Id:      REQUEST,
		Address: address,
		Data:    data,
	}
}

func NewPieceMessage(address string, index, begin uint32, block []byte) *Message {
	data := make([]byte, 8+len(block))
	binary.BigEndian.PutUint32(data[0:4], index)
	binary.BigEndian.PutUint32(data[4:8], begin)
	copy(data[8:], block)

	return &Message{
		Id:      PIECE,
		Address: address,
		Data:    data,
	}
}
