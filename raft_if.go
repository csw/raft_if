package main

import (
	// #cgo CXXFLAGS: -std=c++11
	// #cgo LDFLAGS: -lraft
	// #include "raft_go_if.h"
	"C"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"sync"
	"time"
	"unsafe"
	"github.com/hashicorp/raft"
	// should just be for scaffolding
	"github.com/hashicorp/go-msgpack/codec"
)

// lifted from http://bazaar.launchpad.net/~niemeyer/gommap/trunk/view/head:/gommap.go
type Shm []byte

var ri *raft.Raft
var shm Shm

func main() {
	port := flag.Int("port", 9001, "Raft port to listen on")
	single := flag.Bool("single", false, "Run in single-node mode")
	flag.Parse()

	fmt.Printf("Initializing Raft shared memory.\n")

	nshm, err := ShmInit()
	if (err != nil) {
		panic("Failed to initialize shared memory!")
	}
	shm = nshm
	fmt.Printf("Shared memory initialized.\n")

	conf := raft.DefaultConfig()
	conf.EnableSingleNode = *single
	fsm := &MockFSM{}
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapDir, err := ioutil.TempDir("", "raft")
	if err != nil {
		panic(fmt.Sprintf("err: %v ", err))
	}
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 1, os.Stderr)
	if (err != nil) {
		panic(fmt.Sprintf("Creating snapshot store in %s failed: %v", snapDir, err))
	}

	bindAddr := fmt.Sprintf("127.0.0.1:%d", *port)
	trans, err := raft.NewTCPTransport(bindAddr, nil, 16, 0, nil)
	if err != nil {
		panic(fmt.Sprintf("Binding to %s failed: %v", bindAddr, err))
	}

	peerAddrs := make([]net.Addr, 0, len(flag.Args()))
	for i := 0; i < len(flag.Args()); i++ {
		peer := flag.Args()[i]
		peerAddr, err := net.ResolveTCPAddr("tcp", peer)
		if err != nil {
			panic(fmt.Sprintf("Failed to parse address %s", peer))
		}
		peerAddrs[i] = peerAddr
	}
	peers := &raft.StaticPeers{ StaticPeers: peerAddrs}

	raft, err := raft.NewRaft(conf, fsm, logStore, stableStore, snapStore, peers, trans)
	if (err != nil) {
		panic(fmt.Sprintf("Failed to create Raft instance: %v", err))
	}

	ri = raft
	if StartWorkers() != nil {
		panic(fmt.Sprintf("Failed to start workers: %v", err))
	}

	C.raft_ready()

	for {
		time.Sleep(5*time.Second)
	}
}

func StartWorkers() error {
	go ReportLeaderStatus()
	for i := 0; i < 16; i++ {
		go RunWorker(i)
	}
	return nil
}

func RunWorker(i int) {
	for {
		C.await_call(C.uint32_t(i))
	}
}

func ReportLeaderStatus() {
	leaderCh := ri.LeaderCh()
	for {
		leaderState := <-leaderCh
		C.raft_set_leader(C._Bool(leaderState))
	}
}

func ShmInit() (Shm, error) {
	shared_base := C.raft_shm_init()
	if shared_base == nil {
		panic(fmt.Sprintf("Failed to allocate shared memory!"))
	}

	shared_len := C.raft_shm_size()
	shm := Shm{}
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&shm))
	dh.Data = uintptr(shared_base)
	dh.Len = int(shared_len) // make sure it's under 2^32 bits...
	dh.Cap = dh.Len
	return shm, nil
}

func exercise(r *raft.Raft) {
	if (r.State() == raft.Leader) {
		fmt.Printf("This node is the leader.\n")
		applyF := r.Apply([]byte("foo"), 1*time.Minute)
		if applyF.Error() == nil {
			fmt.Printf("Apply succeeded.\n")
		} else {
			fmt.Printf("Apply failed: %v", applyF.Error())
		}
	}
}

// Interface functions

//export RaftApply
func RaftApply(cmd_offset uintptr, cmd_len uintptr, timeout uint64) (unsafe.Pointer, error) {
	cmd := shm[cmd_offset:cmd_offset+cmd_len]
	fmt.Printf("Applying command (%d bytes): %q\n",
		len(cmd), cmd)
	future := ri.Apply(cmd, time.Duration(timeout))
	if future.Error() == nil {
		fmt.Printf("Command succeeded.\n")
		return nil, nil
	} else {
		fmt.Printf("Command failed: %v\n", future.Error())
		return nil, future.Error()
	}
}

// Temporary scaffolding

// MockFSM is an implementation of the FSM interface, and just stores
// the logs sequentially
type MockFSM struct {
	sync.Mutex
	logs [][]byte
}

type MockSnapshot struct {
	logs     [][]byte
	maxIndex int
}

func (m *MockFSM) Apply(log *raft.Log) interface{} {
	var c_type C.RaftLogType
	var cmd_buf *C.char
	var cmd_len C.size_t

	fmt.Printf("Applying command to C FSM: %q\n", log.Data)

	switch log.Type {
	case raft.LogCommand: c_type = C.RAFT_LOG_COMMAND
	case raft.LogNoop: c_type = C.RAFT_LOG_NOOP
	case raft.LogAddPeer: c_type = C.RAFT_LOG_ADD_PEER
	case raft.LogRemovePeer: c_type = C.RAFT_LOG_REMOVE_PEER
	case raft.LogBarrier: c_type = C.RAFT_LOG_BARRIER
	default:
		panic("Unhandled log type!")
	}
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&log.Data))
	cmd_buf = (*C.char)(unsafe.Pointer(dh.Data))
	cmd_len = C.size_t(dh.Len)
	
	rv := C.raft_fsm_apply(C.uint64_t(log.Index), C.uint64_t(log.Term), c_type, cmd_buf, cmd_len);

	return rv
}

func (m *MockFSM) Snapshot() (raft.FSMSnapshot, error) {
	m.Lock()
	defer m.Unlock()
	return &MockSnapshot{m.logs, len(m.logs)}, nil
}

func (m *MockFSM) Restore(inp io.ReadCloser) error {
	m.Lock()
	defer m.Unlock()
	defer inp.Close()
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(inp, &hd)

	m.logs = nil
	return dec.Decode(&m.logs)
}

func (m *MockSnapshot) Persist(sink raft.SnapshotSink) error {
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(sink, &hd)
	if err := enc.Encode(m.logs[:m.maxIndex]); err != nil {
		sink.Cancel()
		return err
	}
	sink.Close()
	return nil
}

func (m *MockSnapshot) Release() {
}
