package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var Cfg Config
var Stat Status
var filename string

type Config struct {
	TrackerReqParams map[string]string
	R                *rand.Rand
	ListenPort       string
	File             *TorrentFile
	InfoHash         []byte
	PeerId           []byte
	CompactRsp       bool
	DemoAddress      string
	DemoPort         int64
	TrackerInterval  int64
}

func main() {
	flag.Parse()
	setUpCfg()
	Cfg.PeerId = GeneratePeerId(Cfg.R)
	Stat.downloaded = 0
	Stat.uploaded = 0
	Stat.left = Cfg.File.Info.Length
	GenerateTrackerParams(&Cfg, Stat)
	rsp, err := SendTrackerReq(Cfg.File.Announce, Cfg.CompactRsp, Cfg.TrackerReqParams)
	if Cfg.DemoAddress != "" {
		rsp.Peers = []PeerDict{{PeerId: "00000000000000000000", IP: Cfg.DemoAddress, Port: Cfg.DemoPort}}
	}
	if err != nil {
		log.Fatalf("[Bittorrent] Failed to recieve proper tracker response %e", err)
	}

	if rsp.FailureResponse != "" {
		log.Fatalf("[Bittorrent] Failure response from tracker: %s", rsp.FailureResponse)
	}

	m := NewManager(Cfg, rsp)
	ctx, cancel := context.WithCancel(context.Background())
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		log.Printf("\nGot signal [%v] to exit. Wait 10 seconds for resource cleanup\n", sig)
		cancel()
		select {
		case <-sc:
			// send signal again, return directly
			log.Printf("\nGot signal [%v] again to exit.\n", sig)
			os.Exit(1)
		case <-time.After(10 * time.Second):
			log.Print("\nWait 10s for closed, force exit\n")
			os.Exit(1)
		}
	}()
	m.Run(ctx, Cfg)

}

func setUpCfg() {
	var err error
	if filename == "" {
		log.Fatalf("[Bittorrent] Empty filepath %s", filename)
	}

	if len(Cfg.ListenPort) != 4 {
		log.Fatalf("Port must be 4 digits, got: %s", Cfg.ListenPort)
	}

	if _, err := strconv.Atoi(Cfg.ListenPort); err != nil {
		log.Fatalf("Invalid port number: %v", err)
	}

	Cfg.File, Cfg.InfoHash, err = ParseTorrentFile(filename)
	if err != nil {
		log.Fatalf("[Bittorrent] Couldn't parse torrent file: %v", err)
	}
	Cfg.TrackerReqParams = make(map[string]string)
	Cfg.R = getNewRand()
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.StringVar(&filename, "torrentfile", "", "path to torrent file")
	flag.StringVar(&Cfg.ListenPort, "listenport", "8000", "Port for Peer to listen on")
	flag.BoolVar(&Cfg.CompactRsp, "compact", true, "compact response format for tracker")
	flag.StringVar(&Cfg.DemoAddress, "demoaddress", "", "compact response format for tracker")
	flag.Int64Var(&Cfg.DemoPort, "demoport", 8080, "compact response format for tracker")

}
