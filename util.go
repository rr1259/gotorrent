package main

import (
	"crypto/sha1"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const TIMEOUT = time.Second * 5

func GeneratePeerId(r *rand.Rand) []byte {
	randStr := strconv.Itoa(r.Int())
	hash := sha1.New()
	hash.Write([]byte(randStr))
	csum := hash.Sum(nil)
	return csum
}

func getNewRand() *rand.Rand {
	src := rand.NewSource(int64(time.Now().Nanosecond()))
	r := rand.New(src)
	return r
}

func connect(address string) (*net.TCPConn, error) {
	conn, err := net.DialTimeout("tcp", address, TIMEOUT)

	if err != nil {
		return nil, err
	}
	return conn.(*net.TCPConn), err
}

func recvHttpResp(conn net.Conn) ([]byte, error) {
	part := make([]byte, 4096)
	msgRecv := make([]byte, 0, 4096)
	for {
		n, err := conn.Read(part)
		if err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}

		msgRecv = append(msgRecv, part[0:n]...)
	}

	return msgRecv, nil
}

func recvMsg(conn net.Conn, n int) ([]byte, error) {
	ret := make([]byte, n)
	_, err := io.ReadFull(conn, ret)
	if err != nil {
		return nil, err
	}
	
	return ret, nil
}

func sendMsg(conn net.Conn, msg []byte) error {
	conn.SetWriteDeadline(time.Now().Add(TIMEOUT))
	length := len(msg)
	n := 0
	for n < length {
		tmp, err := conn.Write(msg[n:length])
		if err != nil {
			return err
		}

		n += tmp
	}

	return nil
}

func ParseHttpUrlString(url string) (string, error) {
	val := strings.Split(url, "//")
	if len(val) < 2 {
		return "", errors.New("url cannot split //")
	}

	url = val[1]
	url = strings.Split(url, "/")[0]

	return url, nil
}

func GenerateTrackerParams(cfg *Config, stat Status) {
	cfg.TrackerReqParams[INFOHASH] = url.QueryEscape(string(cfg.InfoHash))
	cfg.TrackerReqParams[PEER_ID] = url.QueryEscape(string(cfg.PeerId))
	cfg.TrackerReqParams[PORT] = cfg.ListenPort
	cfg.TrackerReqParams[UPLOADED] = strconv.FormatInt(stat.uploaded, 10)
	cfg.TrackerReqParams[DOWNLOADED] = strconv.FormatInt(stat.downloaded, 10)
	cfg.TrackerReqParams[LEFT] = strconv.FormatInt(stat.left, 10)
	if cfg.CompactRsp {
		cfg.TrackerReqParams[COMPACT] = "1"
	} else {
		cfg.TrackerReqParams[COMPACT] = "0"
	}
	cfg.TrackerReqParams[NO_PEER_ID] = "0"
}

type ConcurrentMap[K comparable, V any] struct {
	m sync.Map
	length	int64
}

func (c *ConcurrentMap[K, V]) Store(key K, value V) {
	c.m.Store(key, value)
	c.length++
}

func (c *ConcurrentMap[K, V]) Load(key K) (V, bool) {
	val, ok := c.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return val.(V), true
}

func (c *ConcurrentMap[K, V]) Delete(key K) {
	c.m.Delete(key)
	c.length--
}

func (c *ConcurrentMap[K, V]) Length() int64{
	return atomic.LoadInt64(&c.length)
}

func (c *ConcurrentMap[K, V]) Range(f func(K, V) bool) {
	c.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}
