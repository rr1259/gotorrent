package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/jackpal/bencode-go"
)


const INFOHASH = "info_hash"
const PEER_ID = "peer_id"
const PORT = "port"
const UPLOADED = "uploaded"
const DOWNLOADED = "downloaded"
const LEFT = "left"
const COMPACT = "compact"
const NO_PEER_ID = "no_peer_id"


type TrackerResp struct {
	FailureResponse string  	`bencode:"failure reason"`
	Complete 		int64 		`bencode:"complete"`
	Incomplete 		int64 		`bencode:"incomplete"`
	Interval 		int64 		`bencode:"interval"`
	Peers 			[]PeerDict 	`bencode:"peers"`
}

type CompactTrackerResp struct {
	FailureResponse string  `bencode:"failure reason"`
	Complete 		int64 	`bencode:"complete"`
	Incomplete 		int64 	`bencode:"incomplete"`
	Interval 		int64 	`bencode:"interval"`
	Peers 			string 	`bencode:"peers"`
}

type PeerDict struct {
	PeerId  string	`bencode:"peer id"`
	IP 		string 	`bencode:"ip"`
	Port 	int64 	`bencodes:"port"`
}

func createHttpReqString(hostUrl string, params map[string]string) string {
	paramArr := []string{}
	for k, v := range params {
		paramArr = append(paramArr, fmt.Sprintf("%s=%s", k, v))
	}

	paramStr := strings.Join(paramArr, "&")
	httpReqStr := fmt.Sprintf("GET /announce?%s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",  paramStr, hostUrl)
	
	return httpReqStr
}

func SendTrackerReq(url string, compact bool, params map[string]string) (*TrackerResp, error) {
	address, err := ParseHttpUrlString(url)
	if err != nil {
		log.Printf("[Bittorrent] error %v", err)
		return nil, err
	}
	httpReq := createHttpReqString(address, params)
	conn, err := connect(address)
	if err != nil {
		log.Printf("[Bittorrent] error %v", err)
		return nil, err
	}
	defer conn.Close()
	err = sendMsg(conn, []byte(httpReq))
	if err != nil {
		log.Printf("[Bittorrent] error %v", err)
		return nil, err
	}

	msg, err := recvHttpResp(conn)
	if err != nil {
		log.Printf("[Bittorrent] error %v", err)
		return nil, err
	}
	
	resp, err := parseTrackerHttpRespString(string(msg), compact)
	if err != nil {
		log.Printf("[Bittorrent] error %v", err)
		return nil, err
	}

	return resp, nil
}

func parseTrackerHttpRespString(resp string, compact bool) (*TrackerResp, error) {
	var parseResp TrackerResp
	var parseCompact CompactTrackerResp
	splithead := strings.Split(resp, "\r\n\r\n")
	content := strings.TrimSpace(splithead[1])

	bufferRead := strings.NewReader(content)
	if compact {
		err := bencode.Unmarshal(bufferRead, &parseCompact)
		if err != nil {
			return nil, err
		}

		parseResp.FailureResponse = parseCompact.FailureResponse
		parseResp.Complete = parseCompact.Complete
		parseResp.Incomplete = parseCompact.Incomplete
		parseResp.Interval = parseCompact.Interval
		peersBytes := []byte(parseCompact.Peers)
		for i := 0; i < len(peersBytes); i+=6 {
			var temp PeerDict
			ip := net.IP(peersBytes[i:i+4])
			temp.IP = ip.String()
			temp.Port = int64(binary.BigEndian.Uint16(peersBytes[i+4:i+6]))
			parseResp.Peers = append(parseResp.Peers, temp)
		}
		return &parseResp, nil
	}
	
	err := bencode.Unmarshal(bufferRead, &parseResp)
	if err != nil {
		return nil, err
	}
	return &parseResp, nil
}