package client

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ao-data/albiondata-client/log"
)

// fileUploader appends every decoded payload to a JSON Lines file instead of
// sending it to a remote ingest server. Each line is:
//
//	{"ts":"2026-11-02T10:00:00Z","topic":"marketorders.ingest","data":{"Orders":[...]}}
//
// Use it with:  albiondata-client.exe -i file://C:/albion/orders.jsonl
//
// This is the "personal capture" mode: no network upload, no POW, and the file
// can be replayed later (-o) or imported into any local database.
type fileUploader struct {
	path string
	mu   sync.Mutex
}

type fileEnvelope struct {
	Ts    string          `json:"ts"`
	Topic string          `json:"topic"`
	Data  json.RawMessage `json:"data"`
}

func newFileUploader(path string) uploader {
	if path == "" {
		log.Error("The file ingest target needs a path, e.g. file://C:/albion/orders.jsonl")
		return &fileUploader{path: "albion-data.jsonl"}
	}

	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Errorf("Could not create the directory for %v: %v", path, err)
		}
	}

	log.Infof("Writing decoded data to %v", path)
	return &fileUploader{path: path}
}

func (u *fileUploader) sendToIngest(body []byte, topic string, state *albionState, identifier string) {
	line, err := json.Marshal(fileEnvelope{
		Ts:    time.Now().UTC().Format(time.RFC3339),
		Topic: topic,
		Data:  json.RawMessage(body),
	})
	if err != nil {
		log.Errorf("Error marshalling the local record for %v: %v", topic, err)
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	f, err := os.OpenFile(u.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Errorf("Error opening %v: %v", u.path, err)
		return
	}
	defer f.Close()

	if _, err := f.Write(append(line, '\n')); err != nil {
		log.Errorf("Error writing to %v: %v", u.path, err)
	}
}
