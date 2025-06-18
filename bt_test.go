package main

import (
	"fmt"
	"net/url"
	"strconv"
	"testing"
)


func TestBencode(t *testing.T) {
	_, val, err := ParseTorrentFile("flatland-http.torrent")
	if (err != nil) {
		err = fmt.Errorf("Error with creating parsing: %s\n", err.Error())
		t.Error(err)
	}

	fmt.Print(url.QueryEscape(string(val)))

	
}

func TestHTTPTrackerRequest(t *testing.T) {
	torf, val, err := ParseTorrentFile("flatland-http.torrent")
	if (err != nil) {
		err = fmt.Errorf("Error with creating parsing: %s\n", err.Error())
		t.Error(err)
		t.Fail()
	}

	testReq := make(map[string]string)
	testReq[INFOHASH] = url.QueryEscape(string(val))
	testReq[PEER_ID] = url.QueryEscape(string(GeneratePeerId(getNewRand()))) 
	testReq[PORT] = "8000"
	testReq[UPLOADED] = "0"
	testReq[DOWNLOADED] = "0"
	testReq[LEFT] = strconv.FormatInt(torf.Info.Length, 10)
	testReq[COMPACT] = "1"
	testReq[NO_PEER_ID] = "0"
	hurl := torf.Announce
	_, err = SendTrackerReq(hurl, true, testReq)
	if err != nil {
		t.Errorf("Error sending tracker req %s", err.Error())
		t.Fail()
	}

	// testReq[COMPACT] = "0"
	// resp1, err := sendTrackerReq(hurl, false, testReq)
	// if err != nil {
	// 	t.Errorf("Error sending trackker req %s", err.Error())
	// 	t.Fail()
	// }
}