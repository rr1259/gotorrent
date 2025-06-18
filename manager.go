package main

/*
fix temp

TODO:
main thread in infinite main select
- listening for messages in channel
- if it receives a PIECE message (this is actually just a block in a piece)
- find the correct PIECE Manager.pieces[]. check if it is downloaded already?
- index at the begin of the block of hte index. copy data
-

UPLOADING TODO:
Go routine running for listening for new connections
Add it to worker, receive the handshake and then send the response.
Send the main channel and add the worker to the map.
start the funneling for that worker.

*/

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const BLOCK_SIZE = 16384
const FLIGHT_WINDOW = 100

type Status struct {
	downloaded int64
	uploaded   int64
	left       int64
}

type Piece struct {
	mut        sync.RWMutex
	downloaded bool
	index      int64
	length     int64
	hash       []byte
	data       []byte
	blocks     map[int]int
}

type Manager struct {
	m            sync.RWMutex
	pieces       []*Piece
	workers      map[string]*Worker
	currPieces   []*PieceStatus
	config       Config
	startTime    time.Time
	workQueue    []*PieceStatus
	openedFiles  map[int]*os.File
	openedFilesM sync.RWMutex
}

type PieceStatus struct {
	PieceIdx         int
	LastDownloadTime time.Time
	CurrDownloading  bool
	Offset           int
}

func NewManager(cfg Config, trackerInfo *TrackerResp) *Manager {
	var newMan Manager
	newMan.config = cfg
	offset := FLIGHT_WINDOW
	newMan.currPieces = make([]*PieceStatus, 0)
	newMan.workQueue = make([]*PieceStatus, 0)

	for i := 0; i < offset; i++ {
		newMan.currPieces = append(newMan.currPieces, &PieceStatus{
			PieceIdx:         i,
			Offset:           offset,
			CurrDownloading:  false,
			LastDownloadTime: time.Now().Add(time.Second * 1),
		})
	}

	numPieces := int64(math.Ceil(float64(cfg.File.Info.Length) / float64(cfg.File.Info.PieceLength)))
	newMan.pieces = make([]*Piece, numPieces)
	newMan.openedFiles = make(map[int]*os.File)

	piecesBytes := []byte(cfg.File.Info.Pieces)
	for i := 0; i < len(piecesBytes); i += 20 {
		idx := i / 20
		newMan.pieces[idx] = &Piece{}
		newMan.pieces[idx].downloaded = false
		newMan.pieces[idx].index = int64(idx)

		if i+20 >= len(piecesBytes) && cfg.File.Info.Length%cfg.File.Info.PieceLength != 0 {
			log.Printf("This is the last block in piece idx: %d %d %d", i, len(piecesBytes), cfg.File.Info.Length%cfg.File.Info.PieceLength)

			newMan.pieces[idx].length = cfg.File.Info.Length % cfg.File.Info.PieceLength
		} else {
			newMan.pieces[idx].length = cfg.File.Info.PieceLength
		}
		newMan.pieces[idx].data = make([]byte, newMan.pieces[idx].length)
		newMan.pieces[idx].hash = piecesBytes[i : i+20]
		newMan.initBlock(idx)
	}

	for i := offset; i < len(newMan.pieces); i++ {
		newMan.workQueue = append(newMan.workQueue, &PieceStatus{
			PieceIdx:         i,
			CurrDownloading:  false,
			LastDownloadTime: time.Now().Add(time.Second * -5),
		})
	}

	newMan.workers = make(map[string]*Worker, len(trackerInfo.Peers))
	for i := 0; i < len(trackerInfo.Peers); i++ {
		key := trackerInfo.Peers[i].IP + ":" + strconv.FormatInt(trackerInfo.Peers[i].Port, 10)
		newMan.workers[key] = NewWorker(key, make(chan *Message, FLIGHT_WINDOW*15), make(chan *Message, FLIGHT_WINDOW*15))
	}
	newMan.startTime = time.Now()
	newMan.createTmpPieceFiles()

	return &newMan
}

func (m *Manager) initBlock(idx int) {
	m.pieces[idx].blocks = make(map[int]int)
	for j := 0; j < int(m.pieces[idx].length); j += BLOCK_SIZE {
		m.pieces[idx].blocks[j] = min(BLOCK_SIZE, int(m.pieces[idx].length)-j)
	}
}

func (m *Manager) createTmpPieceFiles() {
	m.openedFilesM.Lock()
	defer m.openedFilesM.Unlock()

	tempDir := filepath.Join(os.TempDir(), "cmsc417_bt"+m.config.ListenPort)
	err := os.MkdirAll(tempDir, os.ModePerm)
	if err != nil {
		log.Fatalf("UM. couldn't create mp ")
	}

	for i := 0; i < len(m.pieces); i++ {
		tmpFilename := fmt.Sprintf("%d.txt", i)
		tmpFilePath := filepath.Join(tempDir, tmpFilename)

		tmpFile, err := os.Create(tmpFilePath)
		if err != nil {
			log.Fatalf("Couldn't create tmp file %s", tmpFile.Name())
		}
		m.openedFiles[i] = tmpFile
	}
}

func (m *Manager) deleteTmpPieceFiles() {
	m.openedFilesM.Lock()
	defer m.openedFilesM.Unlock()

	// m.openedFiles
	tempDir := filepath.Join(os.TempDir(), "cmsc417_bt"+m.config.ListenPort)
	for i := 0; i < len(m.pieces); i++ {

		tmpFile, ok := m.openedFiles[i]
		if !ok {
			log.Fatalf("Couldn't open tmp file %s", tmpFile.Name())
			return
		}
		tmpFile.Close()

		// delete the tmp file
		tmpFilename := fmt.Sprintf("%d.txt", i)
		tmpFilePath := filepath.Join(tempDir, tmpFilename)
		os.Remove(tmpFilePath)

		delete(m.openedFiles, i)
	}
}

func (m *Manager) combineTmpFilesToOutput(outputFilename string) {
	m.openedFilesM.Lock()
	defer m.openedFilesM.Unlock()

	outFile, err := os.Create(outputFilename)
	if err != nil {
		log.Fatalf("Couldn't create final file")
	}

	defer outFile.Close()

	for i := 0; i < len(m.pieces); i++ {
		tmpFile, ok := m.openedFiles[i]
		tmpFile.Seek(int64(0), 0)
		if !ok {
			log.Fatalf("Couldn't open tmp file %s", tmpFile.Name())
			return
		}

		_, err = io.Copy(outFile, tmpFile)
		if err != nil {
			log.Fatalf("Couldn't write out file %s", tmpFile.Name())
		}
	}
}

func (m *Manager) writeBlockToTmpFile(pieceIdx int, offset int, block []byte) {
	m.openedFilesM.Lock()
	defer m.openedFilesM.Unlock()

	tmpFile, ok := m.openedFiles[pieceIdx]
	if !ok {
		log.Fatalf("Couldn't open tmp file %s", tmpFile.Name())
		return
	}

	_, err := tmpFile.Seek(int64(offset), 0)
	if err != nil {
		log.Fatalf("Couldn't seek tmp file %s", tmpFile.Name())
		return
	}

	_, err = tmpFile.Write(block)
	if err != nil {
		log.Fatalf("Couldn't write tmp file %s", tmpFile.Name())
		return
	}
}

func (m *Manager) readFromTmpFile(pieceIdx int, offset int, length int) []byte {
	m.openedFilesM.Lock()
	defer m.openedFilesM.Unlock()

	tmpFile, ok := m.openedFiles[pieceIdx]
	if !ok {
		log.Fatalf("Couldn't open tmp file %s", tmpFile.Name())
		return nil
	}
	_, err := tmpFile.Seek(int64(offset), 0)
	if err != nil {
		log.Fatalf("Couldn't seek tmp file %s %v", tmpFile.Name(), err)
		return nil
	}

	output := make([]byte, length)
	readBytes, err := tmpFile.Read(output)
	if err != nil && err != io.EOF {
		log.Fatalf("Couldn't read tmp file %s", tmpFile.Name())
		return nil
	}

	if readBytes < length {
		return output[:readBytes]
	}
	return output
}

func (m *Manager) readWholeTmpFile(pieceIdx int) []byte {
	m.openedFilesM.RLock()
	defer m.openedFilesM.RUnlock()
	tempDir := filepath.Join(os.TempDir(), "cmsc417_bt"+m.config.ListenPort)
	tmpFilename := fmt.Sprintf("%d.txt", pieceIdx)
	tmpFilePath := filepath.Join(tempDir, tmpFilename)

	data, err := os.ReadFile(tmpFilePath)
	if err != nil {
		return nil
	}
	return data
}

// Results Channels
func (m *Manager) startFunnel(ctx context.Context, funnelChan chan<- *Message, w *Worker) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case msg, ok := <-w.sendManagerChannel:
				if !ok {
					return
				}
				if msg.Id == DISCONNECT {
					log.Println("FUNNEL DETECTED DISCONNECT")
				}
				funnelChan <- msg
			}
		}
	}()
}

func (m *Manager) handleMessage(msg *Message) {
	// src msg.Address
	switch msg.Id {
	case QUIT:
		// TODO:
		worker, ok := m.workers[msg.Address]
		if ok {
			close(worker.recvManagerChannel)
			delete(m.workers, msg.Address)
		}
		return
	case DISCONNECT:
		// TODO: delete entries???? for that address
		worker, ok := m.workers[msg.Address]
		if ok {
			close(worker.recvManagerChannel)
			delete(m.workers, msg.Address)
		}
		return
	case PIECE:
		// TODO: do we have this piece????
		// if last piece get the hash, validate and set downloaded to true
		// set download to true

		req := msg.GetPieceMessage()
		// invalid block index. yo?
		if req.Idx >= uint32(len(m.pieces)) {
			log.Printf("[Manager] Received PIECE for invalid index %d. Max piece index is %d", req.Idx, len(m.pieces)-1)
			return
		}
		m.pieces[req.Idx].mut.Lock()
		defer m.pieces[req.Idx].mut.Unlock()

		// do we have this block already?????
		if _, ok := m.pieces[req.Idx].blocks[int(req.Begin)]; !ok {
			log.Printf("[Manager] Received duplicate PIECE block")
			return
		}
		pieceStat := m.findPiece(int(req.Idx))
		pieceStat.LastDownloadTime = time.Now()

		// TODO: don't do this????
		copy(m.pieces[req.Idx].data[req.Begin:], req.Block)
		m.writeBlockToTmpFile(int(req.Idx), int(req.Begin), req.Block)

		delete(m.pieces[req.Idx].blocks, int(req.Begin))
		// log.Printf("[Bittorrent] Recieved block of size %d!. left: %d", len(req.Block), len(m.pieces[req.Idx].blocks))

		if len(m.pieces[req.Idx].blocks) == 0 {
			// we have all the blocks... !YOOOOOO

			hash := sha1.New()
			// before:
			hash.Write(m.pieces[req.Idx].data)

			// after:
			// pieceContents := m.readWholeTmpFile(int(req.Idx))
			// hash.Write(pieceContents)

			// ========
			csum := hash.Sum(nil)

			if bytes.Equal(csum, m.pieces[req.Idx].hash) {
				// we good. move on to next piece!!!!
				for _, w := range m.workers {
					w.recvManagerChannel <- NewHaveMessage(w.peerAddress, req.Idx)
				}

				// TODO: write to file??. save it
				// loop through workers
				// delete our pieces bitmap
				piece := m.findPiece(int(req.Idx))
				piece.CurrDownloading = false
				if len(m.workQueue) == 0 {
					piece.PieceIdx += len(m.pieces) + 1
				} else {
					tmpPiece := m.workQueue[0]
					m.workQueue = m.workQueue[1:]
					piece.CurrDownloading = false
					piece.LastDownloadTime = tmpPiece.LastDownloadTime
					piece.PieceIdx = tmpPiece.PieceIdx
				}

				log.Printf("[Manager] currpiece %d", req.Idx)
				m.pieces[req.Idx].downloaded = true
				m.pieceSelect(piece)
				// log.Printf("[Bittorrent] Downloaded Piece %d!", req.Idx)
			} else {
				// try again
				m.initBlock(int(req.Idx))
				m.sendBlockRequests(int(req.Idx))
			}

		}
		// check if map is empty. then do hash stuff. if hash is good. move on to next piece
		// otherwise retry piece

	case INTERESTED:
		// TODO: do this to all of the workers when we finish
	case NOT_INTERESTED:
		// TODO: do this to all of the workers when we finish
	}
}

func (m *Manager) findPiece(idx int) *PieceStatus {
	for _, v := range m.currPieces {
		if v.PieceIdx == idx {
			return v
		}
	}

	return nil
}

func (m *Manager) Run(ctx context.Context, cfg Config) {
	listener, err := net.Listen("tcp", "0.0.0.0:"+m.config.ListenPort)
	if err != nil {
		log.Fatalf("Couldn't listen on port %s: %v", m.config.ListenPort, err)
	}
	newWorkerChan := make(chan *Worker, 10)
	go m.listenForNewPeers(ctx, newWorkerChan, listener)
	generateBitField := m.getBitFieldFunc()

	for _, w := range m.workers {
		w.RunInitiate(ctx, cfg, generateBitField, m.getNeedPieceFunc(), m.getBlockFunc(), false)
	}

	// Peers might send a choke message to us - clear it from the recvBuffer

	// TODO: UNCHOKE??? m.workers
	for _, w := range m.workers {
		w.recvManagerChannel <- NewControlMessage("", UNCHOKE)
	}

	// funnel all of the messages from the workers to manager to funnelChan
	funnelChan := make(chan *Message, FLIGHT_WINDOW*15)
	for _, w := range m.workers {
		m.startFunnel(ctx, funnelChan, w)
	}

	// continuously select on funnelChan and handle message
	// these are messages from the workers to the workers
	// Piece Selection V1 - Sequential
	// call PieceSelection Method here
	timerOpt := time.NewTicker(1 * time.Second)
	defer timerOpt.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-funnelChan:
			m.handleMessage(msg)
		case w := <-newWorkerChan:
			w.RunInitiate(ctx, cfg, generateBitField, m.getNeedPieceFunc(), m.getBlockFunc(), true)
			m.workers[w.peerAddress] = w
			m.startFunnel(ctx, funnelChan, w)
			w.recvManagerChannel <- NewControlMessage("", UNCHOKE)
		case <-timerOpt.C:
			log.Printf("[Bittorrent] piece selection called")
			done := m.doPieceSelection()
			if done {
				log.Println("[Bittorrent] WE ARE DONE************")

				m.combineTmpFilesToOutput(cfg.File.Info.Name)
				break loop
			}
		}
	}

	// seeding mode
	for {
		select {
		case <-ctx.Done():
			m.deleteTmpPieceFiles()
			return
		case msg := <-funnelChan:
			m.handleMessage(msg)
		case w := <-newWorkerChan:
			w.RunInitiate(ctx, cfg, generateBitField, m.getNeedPieceFunc(), m.getBlockFunc(), true)
			m.workers[w.peerAddress] = w
			m.startFunnel(ctx, funnelChan, w)
			w.recvManagerChannel <- NewControlMessage("", UNCHOKE)
		}
	}
}

func (m *Manager) doPieceSelection() bool {

	// 1. Find the next piece to look for
	// 2. Send Requests to workers that have that piece
	outOfBounds := 0
	for _, pieceStatus := range m.currPieces {
		if m.pieceSelect(pieceStatus) {
			outOfBounds++
		}
	}

	return outOfBounds == len(m.currPieces)
}

func (m *Manager) pieceSelect(pieceStatus *PieceStatus) bool {
	timeout := time.Second * 20
	if pieceStatus.CurrDownloading {
		delta := time.Since(pieceStatus.LastDownloadTime)
		if time.Duration(delta.Nanoseconds()) > timeout {
			log.Printf("[Bittorrent] Timed out downloading piece %d", pieceStatus.PieceIdx)
			if m.sendBlockRequests(pieceStatus.PieceIdx) {
				pieceStatus.LastDownloadTime = time.Now()
			}

		}
	} else {
		if pieceStatus.PieceIdx >= len(m.pieces) {
			return true
		} else {
			pieceStatus.CurrDownloading = true
			if pieceStatus.LastDownloadTime.Before(time.Now()) {
				if m.sendBlockRequests(pieceStatus.PieceIdx) {
					pieceStatus.LastDownloadTime = time.Now()
				}
			}
		}
	}

	return false
}

func (m *Manager) sendBlockRequests(idx int) bool {
	// workers that have this piece
	subsetWorkers := make([]*Worker, 0)
	p := m.pieces[idx]
	for _, w := range m.workers {
		if !w.IsChoked() {
			mapPiece := w.GetPieceMap()
			mapPiece.Range(func(pieceIdx int, b bool) bool {
				if pieceIdx == idx {
					subsetWorkers = append(subsetWorkers, w)
				}
				return true
			})
		}
	}
	// sequentialSelection := (int(time.Since(m.startTime).Seconds()) % 15) < 1
	sequentialSelection := time.Since(m.startTime) < time.Second*30
	// sequentialSelection = false
	// Initially send out to all peers to gauge download rate
	if sequentialSelection {
		// log.Printf("SEQUENTIALLLLLLLLL")
		if len(subsetWorkers) == 0 {
			log.Println("[Bittorrent] Found no workers for piece")
			return false
		}
		subsetWorkersIdx := m.config.R.Int() % len(subsetWorkers)

		for offset, length := range p.blocks {
			if offset+length > int(m.config.File.Info.Length) || offset > int(m.config.File.Info.Length) {
				log.Printf("out of bounds: %d %d %d %d", offset, length, offset+length, int(m.config.File.Info.Length))
			}
			select {
			case subsetWorkers[subsetWorkersIdx].recvManagerChannel <- NewRequestMessage("", uint32(idx), uint32(offset), uint32(length)):
			default:
				log.Printf("[Bittorrent] Buffer is full dropping request, sending next time")
			}
			subsetWorkersIdx += 1
			subsetWorkersIdx %= len(subsetWorkers)
		}

		return true
	} else {
		// log.Printf("RANDOMLY")
		// 2: select randomly based on the fastest workers

		sort.Slice(subsetWorkers, func(i, j int) bool {
			return subsetWorkers[i].GetDownloadRate() > subsetWorkers[j].GetDownloadRate()
		})

		// Rank workers on download rate, take top 5
		subsetWorkersTopX := subsetWorkers[:min(len(subsetWorkers), 5)]
		log.Println("[Bittorrent] No. active workers : %d", len(subsetWorkersTopX))
		if len(subsetWorkers) == 0 {
			log.Println("[Bittorrent] Found no workers for piece")
			return false
		}

		workerIndex := 0
		for offset, length := range p.blocks {
			if offset+length > int(m.config.File.Info.Length) || offset > int(m.config.File.Info.Length) {
				log.Printf("out of bounds: %d %d %d %d", offset, length, offset+length, int(m.config.File.Info.Length))
			}
			selectedWorker := subsetWorkersTopX[workerIndex]
			select {
			case selectedWorker.recvManagerChannel <- NewRequestMessage("", uint32(idx), uint32(offset), uint32(length)):
			default:
				log.Printf("[Bittorrent] Buffer is full dropping request, sending next time")
			}

			workerIndex += 1
			workerIndex %= len(subsetWorkersTopX)
		}
	}

	return true
}

func (m *Manager) getBlockFunc() func(idx, offset, length uint32) []byte {
	return func(idx, offset, length uint32) []byte {
		if idx >= uint32(len(m.pieces)) {
			return nil
		}
		if offset > uint32(len(m.pieces[idx].data)) || offset+length > uint32(len(m.pieces[idx].data)) {
			return nil
		}

		m.pieces[idx].mut.RLock()
		defer m.pieces[idx].mut.RUnlock()
		if m.pieces[idx].downloaded {
			// TODO: read from temp file instead
			log.Printf("Requesting data %d %d %d", idx, offset, length)
			return m.readFromTmpFile(int(idx), int(offset), int(length))
			// return m.pieces[idx].data[offset:(offset + length)]
		}

		return nil
	}
}

func (m *Manager) getNeedPieceFunc() func(idx int) bool {
	return func(idx int) bool {
		if idx < 0 || idx >= len(m.pieces) {
			return false
		}
		m.pieces[idx].mut.RLock()
		defer m.pieces[idx].mut.RUnlock()
		if m.pieces[idx].downloaded {
			return false
		}

		return true
	}
}

func (m *Manager) getBitFieldFunc() func() []byte {
	return func() []byte {
		m.m.RLock()
		defer m.m.RUnlock()
		numBytes := int64(math.Ceil(float64(len(m.pieces)) / 8))
		ret := make([]byte, numBytes)
		for i, piece := range m.pieces {
			var idx int64 = int64(i / 8)
			bitPos := i % 8
			if piece.downloaded {
				ret[idx] |= 1 << (7 - bitPos)
			}
		}

		return ret
	}
}

func (m *Manager) listenForNewPeers(ctx context.Context, workerPipe chan *Worker, listener net.Listener) {
	for {
		select {
		case <-ctx.Done():
			close(workerPipe)
			return
		default:
			newConn, err := listener.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %s", err)
				continue
			}
			log.Printf("Accepted connection from %s", newConn.RemoteAddr().String())
			recvChan := make(chan *Message, FLIGHT_WINDOW*15)
			sendChan := make(chan *Message, FLIGHT_WINDOW*15)
			w := NewWorker(newConn.RemoteAddr().String(), recvChan, sendChan)
			if _, ok := newConn.(*net.TCPConn); ok {
				w.peerConn = newConn.(*net.TCPConn)
			} else {
				log.Printf("Error accepting connection: %s", err)
				newConn.Close()
				continue
			}

			workerPipe <- w
		}
	}
}
