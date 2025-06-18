package main

import (
	"bufio"
	"crypto/sha1"
	"os"
	"strings"

	"github.com/jackpal/bencode-go"
)

type TorrentFile struct {
	Announce string 	`bencode:"announce"`		// announce url
	Info Info   		`bencode:"info"`			// see below
}

type Info struct {
	Length int64  		`bencode:"length"`       	// length of file
	Name   string 		`bencode:"name"`		   	// name of file
	PieceLength  int64  `bencode:"piece length"`	// length of piece
	Pieces string 		`bencode:"pieces"`			// string containing hash of all pieces
}

type torrentFileInfoHash struct {
	Info string `bencode:"info"`
}

func ParseTorrentFile(filename string) (*TorrentFile, []byte, error) {
	var parseTorr TorrentFile 
	file, err := os.Open(filename) 
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	bufferRead := bufio.NewReader(file)
	err = bencode.Unmarshal(bufferRead, &parseTorr)
	if err != nil {
		return nil, nil, err
	}
	b := new(strings.Builder)
	err = bencode.Marshal(b, parseTorr.Info)
	if err != nil {
		return nil, nil, err
	}

	hash := sha1.New()
	hash.Write([]byte(b.String()))
	csum := hash.Sum(nil)
	
	return &parseTorr, csum, nil
}