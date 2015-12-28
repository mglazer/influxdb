package meta_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/influxdb/influxdb"
	"github.com/influxdb/influxdb/influxql"
	"github.com/influxdb/influxdb/services/meta"
	"github.com/influxdb/influxdb/services/meta/internal"
	"github.com/influxdb/influxdb/tcp"

	"github.com/gogo/protobuf/proto"
)

// Test the ping endpoint.
func TestMetaService_PingEndpoint(t *testing.T) {
	t.Parallel()

	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
	s := newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	url, err := url.Parse(s.URL())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Head("http://" + url.String() + "/ping")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status:\n\texp: %d\n\tgot: %d\n", http.StatusOK, resp.StatusCode)
	}
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMetaService_CreateDatabase(t *testing.T) {
	cfg := newConfig()
	defer os.RemoveAll(cfg.Dir)
	s := newService(cfg)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}

	c := meta.NewClient([]string{s.URL()}, false)
	if err := c.Open(); err != nil {
		t.Fatalf(err.Error())
	}
	defer c.Close()

	c.ExecuteStatement(mustParseStatement("CREATE DATABASE FOO"))
	db, err := c.Database("foo")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if db.Name != "foo" {
		t.Fatalf("db name wrong: %s", db.Name)
	}
}

// Test long poll of snapshot.
// Clients will make a long poll request for a snapshot update by passing their
// current snapshot index.  The meta service will respond to the request when
// its snapshot index exceeds the client's snapshot index.
// func TestMetaService_LongPoll(t *testing.T) {
// 	t.Parallel()

// 	cfg := newConfig()
// 	defer os.RemoveAll(cfg.Dir)
// 	s := newService(cfg)
// 	if err := s.Open(); err != nil {
// 		t.Fatal(err)
// 	}

// 	before, err := snapshot(s, 0)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	node := before.Node(1)
// 	if node != nil {
// 		t.Fatal("expected <nil> but got a node")
// 	}

// 	// Start a long poll request for a snapshot update.
// 	ch := make(chan *meta.Data)
// 	errch := make(chan error)
// 	go func() {
// 		after, err := snapshot(s, 1)
// 		if err != nil {
// 			errch <- err
// 		}
// 		ch <- after
// 	}()

// 	// Fire off an update after a delay.
// 	host := "127.0.0.1"
// 	update := make(chan struct{})
// 	go func() {
// 		<-update
// 		cmdval := &internal.CreateNodeCommand{
// 			Host: proto.String(host),
// 			Rand: proto.Uint64(42),
// 		}
// 		if err := exec(s, internal.Command_CreateNodeCommand, internal.E_CreateNodeCommand_Command, cmdval); err != nil {
// 			errch <- err
// 		}
// 	}()

// 	for i := 0; i < 2; i++ {
// 		select {
// 		case after := <-ch:
// 			node = after.Node(1)
// 			if node == nil {
// 				t.Fatal("expected node but got <nil>")
// 			} else if node.Host != host {
// 				t.Fatalf("unexpected host:\n\texp: %s\n\tgot: %s\n", host, node.Host)
// 			}
// 		case err := <-errch:
// 			t.Fatal(err)
// 		case <-time.After(time.Second):
// 			// First time through the loop it should time out because update hasn't happened.
// 			if i == 0 {
// 				// Signal the update
// 				update <- struct{}{}
// 			} else {
// 				t.Fatal("timed out waiting for snapshot update")
// 			}
// 		}
// 	}
// }

func newConfig() *meta.Config {
	cfg := meta.NewConfig()
	cfg.BindAddress = "127.0.0.1:0"
	cfg.Dir = testTempDir(2)
	return cfg
}

func testTempDir(skip int) string {
	// Get name of the calling function.
	pc, _, _, ok := runtime.Caller(skip)
	if !ok {
		panic("failed to get name of test function")
	}
	_, prefix := path.Split(runtime.FuncForPC(pc).Name())
	// Make a temp dir prefixed with calling function's name.
	dir, err := ioutil.TempDir("/tmp", prefix)
	if err != nil {
		panic(err)
	}
	return dir
}

func mustProtoMarshal(v proto.Message) []byte {
	b, err := proto.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func snapshot(s *meta.Service, index int) (*meta.Data, error) {
	url := fmt.Sprintf("http://%s?index=%d", s.URL(), index)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	data := &meta.Data{}
	if err := data.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return data, nil
}

func exec(s *meta.Service, typ internal.Command_Type, desc *proto.ExtensionDesc, value interface{}) error {
	// Create command.
	cmd := &internal.Command{Type: &typ}
	if err := proto.SetExtension(cmd, desc, value); err != nil {
		panic(err)
	}
	b := mustProtoMarshal(cmd)
	url := fmt.Sprintf("http://%s/execute", s.URL())
	resp, err := http.Post(url, "application/octet-stream", bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected result:\n\texp: %d\n\tgot: %d\n", http.StatusOK, resp.StatusCode)
	}
	return nil
}

func newService(cfg *meta.Config) *meta.Service {
	// Open shared TCP connection.
	fmt.Println(cfg.BindAddress)
	ln, err := net.Listen("tcp", cfg.BindAddress)
	if err != nil {
		panic(err)
	}

	// Multiplex listener.
	mux := tcp.NewMux()

	s := meta.NewService(cfg, &influxdb.Node{})
	s.RaftListener = mux.Listen(meta.MuxHeader)

	go mux.Serve(ln)

	return s
}

func mustParseStatement(s string) influxql.Statement {
	stmt, err := influxql.ParseStatement(s)
	if err != nil {
		panic(err)
	}
	return stmt
}
