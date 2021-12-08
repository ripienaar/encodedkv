package encodedkv

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestCodec(t *testing.T) {
	codec := &SlashCodec{}

	res, _ := codec.EncodeKey("/foo")
	if res != "Zm9v" {
		t.Fatalf("invalid: %s", res)
	}

	res, _ = codec.DecodeKey("Zm9v")
	if res != "/foo" {
		t.Fatalf("invalid: %s", res)
	}

	res, _ = codec.EncodeRange("/foo/bar")
	if res != "Zm9v.YmFy.>" {
		t.Fatalf("invalid: %s", res)
	}
}

func TestSlashEncoder(t *testing.T) {
	withJetStream(t, func(_ *server.Server, nc *nats.Conn, js nats.JetStreamContext) {
		b, err := js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  "TEST",
			History: 5,
		})
		if err != nil {
			t.Fatalf("create failed: %s", err)
		}

		eb := NewEncodedKV(b, &SlashCodec{})
		_, err = eb.PutString("/this/is/a.test", "hello world")
		if err != nil {
			t.Fatalf("put failed: %s", err)
		}

		e, err := eb.Get("/this/is/a.test")
		if err != nil {
			t.Fatalf("get failed: %s", err)
		}

		if !bytes.Equal(e.Value(), []byte("hello world")) {
			t.Fatalf("wrong value: %q", e.Value())
		}

		if e.Key() != "/this/is/a.test" {
			t.Fatalf("invalid key: %v", e.Key())
		}

		watcher, err := eb.Watch("/some/other/dir")
		if err != nil {
			t.Fatalf("watchr failed: %s", err)
		}
		if entry := <-watcher.Updates(); entry != nil {
			t.Fatalf("got an entry when none should have been received: %#v", entry)
		}
		watcher.Stop()

		watcher, err = eb.Watch("/this/is")
		if err != nil {
			t.Fatalf("watcher failed: %s", err)
		}
		for entry := range watcher.Updates() {
			if entry == nil {
				t.Fatalf("got nil")
			}

			if entry.Key() != "/this/is/a.test" {
				t.Fatalf("invalid key: %s", entry.Key())
			}

			if entry.Delta() == 0 {
				watcher.Stop()
				break
			}
		}
	})
}

func withJetStream(t *testing.T, cb func(*server.Server, *nats.Conn, nats.JetStreamContext)) {
	t.Helper()

	srv, nc, js := startJSServer(t)
	defer os.RemoveAll(srv.JetStreamConfig().StoreDir)
	defer srv.Shutdown()

	cb(srv, nc, js)
}

func startJSServer(t *testing.T) (*server.Server, *nats.Conn, nats.JetStreamContext) {
	t.Helper()

	d, err := ioutil.TempDir("", "jstest")
	if err != nil {
		t.Fatalf("temp dir could not be made: %s", err)
	}

	opts := &server.Options{
		ServerName: "test.example.net",
		JetStream:  true,
		StoreDir:   d,
		Port:       -1,
		Host:       "localhost",
		LogFile:    "/tmp/server.log",
		// Trace:        true,
		// TraceVerbose: true,
		Cluster: server.ClusterOpts{Name: "gotest"},
	}

	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("server start failed: %s", err)
	}

	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatalf("nats server did not start")
	}

	// s.ConfigureLogger()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("client start failed: %s", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, nil, nil
	}

	return s, nc, js
}
