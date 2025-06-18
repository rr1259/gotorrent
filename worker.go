package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const WORKERTIMEOUT time.Duration = time.Minute * 2
const WORKER_KEEP_ALIVE_INTERVAL time.Duration = time.Second * 5

// With data structure that keeps of what piece is downloaded and what not
// array struct {piecehash, downloaded boolean, piecelength, idx of it}

/* With peer we first connect
If connect fails - we send to main thread quit
 We send handshake - if handshake fails either they quit connect or we close because incorrect peer id
 	Send quit message back
We encode bitfield message based on data structure
We then send choke
We the recv hoping for bitfield but also okay if not
If recieve send bitfield back
After send choke message, recieve choke message
main thread figures piece selection - random select of 4 pieces
	Unchoke the other peer
	IF peer sends us an unchoke message (telling us we can download from peer), start downloading
	Peer also checks if has been unchoked
	if interested and unchoked, start requesting
main thread tells all workers connected to a peer who have the piece to be interested and unchoke the peer
Polling for minute for pieces download - go into snubbing - tells worker to choke peer
*/

type Worker struct {
	piecesMap          ConcurrentMap[int, bool] // Map of the pieces we want from the peer from the peer and DONT HAVE
	peerConn           *net.TCPConn
	mut                sync.RWMutex
	isDownloading      bool
	downloadRate       float64
	optUnChoke         bool
	regUnChoke         bool
	amChoking          bool // worker is choking the peer
	amInterested       bool // worker is interested in the peer
	peerChoking        bool // peer is choking the worker
	peerInterested     bool // peer is interested in the worker (wants to download from us)
	peerAddress        string
	recvManagerChannel chan *Message
	sendManagerChannel chan *Message
	timeLastRequest    time.Time
	sendMsgChannel     chan *Message
}

func NewWorker(peerAddress string, recvChannel chan *Message, sendChannel chan *Message) (w *Worker) {
	w = &Worker{}
	w.amChoking = true
	w.peerChoking = true
	w.peerAddress = peerAddress
	w.recvManagerChannel = recvChannel
	w.sendManagerChannel = sendChannel

	return w
}

func (w *Worker) RunInitiate(ctx context.Context, cfg Config, generateBitField func() []byte, needPiece func(idx int) bool, getBlock func(idx, offset, length uint32) []byte, reciever bool) {
	go w.runInitiate(ctx, cfg, generateBitField, needPiece, getBlock, reciever)
}

func (w *Worker) runInitiate(ctx context.Context, cfg Config, generateBitField func() []byte, needPiece func(idx int) bool, getBlock func(idx, offset, length uint32) []byte, reciever bool) {
	defer close(w.sendManagerChannel)
	var err error
	if !reciever {
		w.peerConn, err = connect(w.peerAddress)
		if err != nil {
			log.Printf("[Bittorrent] failed to connect to peer %s with error %v\n", w.peerAddress, err)
			w.sendManagerChannel <- &Message{MessageId(QUIT), w.peerAddress, nil}
			return
		}

		defer w.peerConn.Close()

		err = w.sendHandShake(cfg.InfoHash, cfg.PeerId)
		if err != nil {
			log.Printf("[Bittorrent] failed to send handshake %v\n", err)
			w.sendManagerChannel <- &Message{MessageId(QUIT), w.peerAddress, nil}
			return
		}

		err = w.recvReturnHandShake(cfg)
		if err != nil {
			log.Printf("[Bittorrent] failed to recieve correct handshake %v\n", err)
			w.sendManagerChannel <- &Message{MessageId(QUIT), w.peerAddress, nil}
			return
		}
	} else {
		defer w.peerConn.Close()
		err = w.recvReturnHandShake(cfg)
		if err != nil {
			log.Printf("[Bittorrent] failed to recieve correct handshake %v\n", err)
			w.sendManagerChannel <- &Message{MessageId(QUIT), w.peerAddress, nil}
			return
		}

		err = w.sendHandShake(cfg.InfoHash, cfg.PeerId)
		if err != nil {
			log.Printf("[Bittorrent] failed to send handshake %v\n", err)
			w.sendManagerChannel <- &Message{MessageId(QUIT), w.peerAddress, nil}
			return
		}
	}
	err = w.sendBitField(generateBitField)
	if err != nil {
		log.Printf("[Bittorrent] failed to recieve correct handshake %v\n", err)
		w.sendManagerChannel <- &Message{MessageId(QUIT), w.peerAddress, nil}
		return
	}
	w.maintainPeer(ctx, cfg, needPiece, getBlock)

}

func (w *Worker) maintainPeer(ctx context.Context, cfg Config, needPiece func(idx int) bool, getBlock func(idx, offset, length uint32) []byte) {
	recvMsgChannel := make(chan *Message, FLIGHT_WINDOW*15)
	w.sendMsgChannel = make(chan *Message, FLIGHT_WINDOW*15)
	errorChan := make(chan *Message, 30)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() { w.sendManagerChannel <- &Message{MessageId(DISCONNECT), w.peerAddress, nil} }()
	go w.sendMsgRoutine(ctx, w.sendMsgChannel, errorChan)
	go w.recievePeerMessage(ctx, recvMsgChannel)
	timer := time.NewTimer(WORKERTIMEOUT)
	keepAliveTimeout := time.Minute
	keepAliveTimer := time.NewTimer(keepAliveTimeout)
	defer timer.Stop()
	defer keepAliveTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-recvMsgChannel:
			if msg.Id == DISCONNECT {
				log.Printf("Disconnected at maintainPeer %s", w.peerAddress)
				w.sendManagerChannel <- msg
				return
			}
			timer.Reset(WORKERTIMEOUT)
			err := w.handleTcpMessage(msg, needPiece, getBlock)
			if err != nil {
				log.Printf("Error with handling TCP message %v from worker %s", err, w.peerAddress)
				return
			}
		case <-errorChan:
			return
		case msg := <-w.recvManagerChannel:
			err := w.handleManagerMessage(msg)
			if err != nil {
				log.Printf("[Bittorrent] Error with handling Manager message %v from manager %s", err, w.peerAddress)
				return
			}
		case <-keepAliveTimer.C:
			err := w.sendMsg(NewControlMessage("", KEEP_ALIVE))
			if err != nil {
				log.Printf("[Bittorrent] Error with keep alive %v with address %s", err, w.peerAddress)
				return
			}
			keepAliveTimer.Reset(keepAliveTimeout)
		case <-timer.C:
			log.Printf("[Bittorrent] Haven't recieved keep alive message")
			return
		}
	}
}

func (w *Worker) sendMsgRoutine(ctx context.Context, sendChan chan *Message, errChan chan *Message) {
	defer close(sendChan)
	defer close(errChan)

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-sendChan:
			var buf []byte
			if msg.Data == nil && msg.Id != KEEP_ALIVE {
				buf = make([]byte, 5)
				binary.BigEndian.PutUint32(buf[0:4], 1)
				buf[4] = byte(uint8(msg.Id))
			} else if msg.Data == nil && msg.Id == KEEP_ALIVE {
				buf = make([]byte, 4)
				binary.BigEndian.PutUint32(buf[0:4], 0)
			} else {
				buf = make([]byte, 5+len(msg.Data))
				binary.BigEndian.PutUint32(buf[0:4], uint32(1+len(msg.Data)))
				buf[4] = byte(uint8(msg.Id))
				copy(buf[5:], msg.Data)
			}

			if msg.Id == 4 {
				// log.Printf("have data: %x", buf)
			}
			if msg.Id == KEEP_ALIVE {
				log.Printf("data KEEP_ALIVE: %x", buf)
			}

			if msg.Id == REQUEST {
				w.mut.Lock()
				w.timeLastRequest = time.Now()
				w.mut.Unlock()
			}

			err := sendMsg(w.peerConn, buf)
			if err != nil {
				log.Printf("[Bittorrent Worker] Problem sending to peer %s, disconnecting", w.peerAddress)
				errChan <- &Message{Id: DISCONNECT}
				return
			}
		}
	}
}

func (w *Worker) handleManagerMessage(msg *Message) error {
	switch msg.Id {
	case HAVE:
		// We first check if it exists in the concurrent hashmap  piecesMap
		// If its does we remove and delete from concurrent hashmap piecesMap
		// we check if piecesMap is empty after delete - if it is then we have mark uninterested to both the peer and manager so manager can run choke alg

		err := w.sendMsg(msg)
		if err != nil {
			err = fmt.Errorf("error propagating have: %v", err)
			return err
		}

		haveMsg := msg.GetHaveMessage()

		if _, ok := w.piecesMap.Load(int(haveMsg.Idx)); ok {
			w.piecesMap.Delete(int(haveMsg.Idx))

			// we have everything the peer has now. we are not longer interested in th
			if w.piecesMap.length == 0 {
				unIntMsg := NewControlMessage(w.peerAddress, NOT_INTERESTED)
				w.mut.Lock()
				w.amInterested = false
				w.mut.Unlock()
				err := w.sendMsg(unIntMsg)
				if err != nil {
					err = fmt.Errorf("error propagating not interested message: %v", err)
					return err
				}
			}
		}
	case CHOKE:
		w.mut.Lock()
		w.peerChoking = true
		w.mut.Unlock()
		err := w.sendMsg(msg)
		if err != nil {
			err = fmt.Errorf("error sending request out: %v", err)
			return err
		}

	case UNCHOKE:
		w.mut.Lock()
		w.peerChoking = false
		w.mut.Unlock()
		err := w.sendMsg(msg)
		if err != nil {
			err = fmt.Errorf("error sending request out: %v", err)
			return err
		}
	case REQUEST:
		// Manager TELLS which blocks to request. we send the request to Peer
		if w.amChoking {
			log.Printf("[Bittorrent] Can't send request being choked")
			break
		}
		err := w.sendMsg(msg)
		if err != nil {
			err = fmt.Errorf("error sending request out: %v", err)
			return err
		}
	}

	return nil
}

func (w *Worker) handleTcpMessage(msg *Message, needPiece func(idx int) bool, getBlock func(idx, offset, length uint32) []byte) error {
	// log.Printf("Got message id: %d len: %d addr: %s", msg.Id, len(msg.Data), w.peerAddress)

	switch msg.Id {
	case CHOKE:
		w.mut.Lock()
		w.amChoking = true
		w.mut.Unlock()
	case UNCHOKE:
		w.mut.Lock()
		w.amChoking = false
		w.mut.Unlock()
	case INTERESTED:
		w.mut.Lock()
		if !w.peerInterested {
			w.sendManagerChannel <- NewControlMessage(w.peerAddress, INTERESTED)
			log.Printf("[Bittorrent] they are interested in downloading from us")
		}
		w.peerInterested = true
		w.mut.Unlock()
	case NOT_INTERESTED:
		w.mut.Lock()
		if w.peerInterested {
			w.sendManagerChannel <- NewControlMessage(w.peerAddress, NOT_INTERESTED)
		}
		w.peerInterested = false
		w.mut.Unlock()
	case HAVE:
		haveMsg := msg.GetHaveMessage()
		err := w.determineInterest(int(haveMsg.Idx), needPiece)
		if err != nil {
			err = fmt.Errorf("error with have message %v", err)
			return err
		}
	case BITFIELD:
		log.Printf("[Bittorrent] Sent us a bitfield")
		for byteIndex, b := range msg.Data {
			for bit := 0; bit < 8; bit++ {
				if b&(1<<(7-bit)) != 0 {
					pieceIndex := byteIndex*8 + bit
					err := w.determineInterest(pieceIndex, needPiece)
					if err != nil {
						err = fmt.Errorf("error with bitfield message %v", err)
						return err
					}
				}
			}
		}
	case REQUEST:
		log.Printf("[Bittorrent] Got Request Message")
		if w.IsChokingPeer() {
			log.Printf("[Bittorrent] Is choking peer wont send piece")
			return nil
		}
		req := msg.GetRequestMessage()
		data := getBlock(req.Idx, req.Begin, req.Length)
		if data != nil {
			log.Printf("[Bittorrent] Data isn't nil can send piece")
			piece := NewPieceMessage("", req.Idx, req.Begin, data)
			err := w.sendMsg(piece)
			if err != nil {
				err = fmt.Errorf("error with handling sending message %v", err)
				return err
			}
		}
	case PIECE:
		w.mut.Lock()
		timeDiff := time.Since(w.timeLastRequest)
		if timeDiff > 0 {
			w.downloadRate = float64(len(msg.Data)) / float64(timeDiff.Milliseconds())
		}
		w.mut.Unlock()
		select {
		case w.sendManagerChannel <- msg:
		default:
			log.Printf("[Bittorrent] Buffer is full dropping piece to manager")
		}

	case KEEP_ALIVE:
		log.Println("Keep alive detected!")
	default:
		log.Printf("Unknown message with id: %d len: %d", msg.Id, len(msg.Data))
	}

	return nil
}

func (w *Worker) determineInterest(idx int, needPiece func(idx int) bool) error {
	if needPiece(idx) {
		w.piecesMap.Store(int(idx), true)
		w.mut.Lock()
		defer w.mut.Unlock()
		if !w.amInterested {
			w.amInterested = true
			intMsg := NewControlMessage(w.peerAddress, INTERESTED)
			w.sendManagerChannel <- intMsg
			err := w.sendMsg(intMsg)
			return err
		}
	}

	return nil
}

// Sending the message to peer connected on a worker
func (w *Worker) sendMsg(msg *Message) error {
	w.sendMsgChannel <- msg
	return nil
}

func (w *Worker) IsChokingPeer() bool {
	w.mut.RLock()
	defer w.mut.RUnlock()
	return w.peerChoking
}

func (w *Worker) recievePeerMessage(ctx context.Context, forwardChan chan<- *Message) {
	defer close(forwardChan)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var err error
			lenBytes, err := recvMsg(w.peerConn, 4)
			if err != nil {
				forwardChan <- &Message{MessageId(DISCONNECT), w.peerAddress, nil}
				log.Printf("[Bittorrent] lenBytes disconnected in recievePeerMessage of address: %s with error %v", w.peerAddress, err)
				return
			}
			lenMessage := binary.BigEndian.Uint32(lenBytes)
			if lenMessage == 0 {
				forwardChan <- &Message{MessageId(KEEP_ALIVE), w.peerAddress, nil}
				continue
			}

			idByte, err := recvMsg(w.peerConn, 1)
			if err != nil {
				forwardChan <- &Message{MessageId(DISCONNECT), w.peerAddress, nil}
				log.Printf("[Bittorrent] idByte disconnected in recievePeerMessage of address: %s with error %v", w.peerAddress, err)
				return
			}

			id := int(idByte[0])

			if lenMessage-1 == 0 {
				forwardChan <- &Message{MessageId(id), w.peerAddress, nil}

			}

			dataBuf, err := recvMsg(w.peerConn, int(lenMessage-1))
			if err != nil {
				forwardChan <- &Message{MessageId(DISCONNECT), w.peerAddress, nil}
				log.Printf("[Bittorrent] dataBuf disconnected in recievePeerMessage of address: %s with error %v", w.peerAddress, err)
				return
			}

			forwardChan <- &Message{MessageId(id), w.peerAddress, dataBuf}

		}
	}
}

func (w *Worker) sendBitField(generateBitField func() []byte) error {
	bitField := generateBitField()
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(1+len(bitField)))
	msg := make([]byte, 0)
	msg = append(msg, length...)
	msg = append(msg, uint8(5))
	msg = append(msg, bitField...)
	err := sendMsg(w.peerConn, msg)
	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) sendHandShake(infoHash []byte, peerId []byte) error {
	pstr := "BitTorrent protocol"
	pstrLen := byte(len(pstr))
	msg := make([]byte, 0)
	msg = append(msg, pstrLen)
	msg = append(msg, []byte(pstr)...)
	msg = append(msg, make([]byte, 8)...)
	msg = append(msg, infoHash...)
	msg = append(msg, peerId...)
	err := sendMsg(w.peerConn, msg)
	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) recvReturnHandShake(cfg Config) error {
	var err error
	var pstrlen []byte
	var pstr []byte
	var info_hash []byte
	pstrlen, err = recvMsg(w.peerConn, 1)
	if err != nil {
		return err
	}
	pstr, err = recvMsg(w.peerConn, int(uint8(pstrlen[0])))
	if err != nil {
		return err
	}

	if string(pstr) != "BitTorrent protocol" {
		log.Printf("Recieved wrong p2p protocol %s", string(pstr))
		return errors.New("wrong p2p protocol")
	}

	_, err = recvMsg(w.peerConn, 8)
	if err != nil {
		return err
	}

	info_hash, err = recvMsg(w.peerConn, 20)
	if err != nil {
		return err
	}
	if !bytes.Equal(info_hash, cfg.InfoHash) {
		return errors.New("infohash validation failed")
	}

	// Just ignore if compact or all the time it will be right
	_, err = recvMsg(w.peerConn, 20)
	if err != nil {
		return err
	}

	return nil
}

func (w *Worker) IsChoked() bool {
	w.mut.RLock()
	defer w.mut.RUnlock()
	return w.amChoking
}

func (w *Worker) GetDownloadRate() float64 {
	w.mut.RLock()
	defer w.mut.RUnlock()
	return w.downloadRate
}

func (w *Worker) GetPieceMap() *ConcurrentMap[int, bool] {
	return &w.piecesMap
}
