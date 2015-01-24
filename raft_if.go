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
	"log"
	"net"
	"os"
	"reflect"
	"sync"
	"syscall"
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
var lg *log.Logger

func main() {
	pid  := os.Getpid()
	ppid := os.Getppid()
	lg = log.New(os.Stderr, fmt.Sprintf("raft_if [%d] ", pid), log.LstdFlags)

	port := flag.Int("port", 9001, "Raft port to listen on")
	single := flag.Bool("single", false, "Run in single-node mode")
	flag.Parse()

	lg.Printf("Starting Raft service for parent PID %d.\n", ppid)
	lg.Println("Initializing Raft shared memory.")

	nshm, err := ShmInit()
	if (err != nil) {
		lg.Panicln("Failed to initialize shared memory!")
	}
	shm = nshm
	lg.Printf("Shared memory initialized.\n")
	go WatchParent(ppid)

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
		lg.Panicf("Creating snapshot store in %s failed: %v",
			snapDir, err)
	}

	bindAddr := fmt.Sprintf("127.0.0.1:%d", *port)
	trans, err := raft.NewTCPTransport(bindAddr, nil, 16, 0, nil)
	if err != nil {
		lg.Panicf("Binding to %s failed: %v", bindAddr, err)
	}

	peerAddrs := make([]net.Addr, 0, len(flag.Args()))
	for i := 0; i < len(flag.Args()); i++ {
		peer := flag.Args()[i]
		peerAddr, err := net.ResolveTCPAddr("tcp", peer)
		if err != nil {
			lg.Panicf("Failed to parse address %s", peer)
		}
		peerAddrs[i] = peerAddr
	}
	peers := &raft.StaticPeers{ StaticPeers: peerAddrs}

	raft, err := raft.NewRaft(conf, fsm, logStore, stableStore, snapStore, peers, trans)
	if (err != nil) {
		lg.Panicf("Failed to create Raft instance: %v", err)
	}

	ri = raft
	if StartWorkers() != nil {
		lg.Panicf("Failed to start workers: %v", err)
	}

	C.raft_ready()

	for {
		time.Sleep(5*time.Second)
	}
}

func StartWorkers() error {
	go ReportLeaderStatus()
	return nil
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
		lg.Panicf("Failed to allocate shared memory!")
	}

	shared_len := C.raft_shm_size()
	shm := Shm{}
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&shm))
	dh.Data = uintptr(shared_base)
	dh.Len = int(shared_len) // make sure it's under 2^32 bits...
	dh.Cap = dh.Len
	return shm, nil
}

func WatchParent(ppid int) {
	for {
		err := syscall.Kill(ppid, 0)
		if (err != nil) {
			lg.Printf("Parent process %d has exited!\n", ppid)
			OnParentExit()
		}
		time.Sleep(1*time.Second)
	}
}

func OnParentExit() {
	future := ri.Shutdown()
	lg.Println("Waiting for Raft to shut down...");
	if (future.Error() != nil) {
		lg.Fatalf("Error during shutdown: %v\n")
	} else {
		lg.Fatalln("Shutdown due to parent exit complete.")
	}
}

// Interface functions

// note: multiple return value structs have members r0, r1, ...
// errors are represented as a GoInterface struct
// typedef struct { void *data; GoInt len; GoInt cap; } GoSlice
// typedef struct { char *p; GoInt n; } GoString

//export RaftApply
func RaftApply(cmd_offset uintptr, cmd_len uintptr, timeout uint64) (unsafe.Pointer, C.RaftError) {
	cmd := shm[cmd_offset:cmd_offset+cmd_len]
	//ri.logger.Printf("[INFO] Applying command (%d bytes): %q\n", len(cmd), cmd)
	future := ri.Apply(cmd, time.Duration(timeout))
	if future.Error() == nil {
		return nil, C.RAFT_SUCCESS
	} else {
		lg.Printf("Command failed: %v\n", future.Error())
		return nil, TranslateRaftError(future.Error())
	}
}

//export TranslateRaftError
func TranslateRaftError(err error) C.RaftError {
	switch err {
	case nil: return C.RAFT_SUCCESS
	case raft.ErrLeader: return C.RAFT_E_LEADER
	case raft.ErrNotLeader: return C.RAFT_E_NOT_LEADER
	case raft.ErrLeadershipLost: return C.RAFT_E_LEADERSHIP_LOST
	case raft.ErrRaftShutdown: return C.RAFT_E_SHUTDOWN
	case raft.ErrEnqueueTimeout: return C.RAFT_E_ENQUEUE_TIMEOUT
	case raft.ErrKnownPeer: return C.RAFT_E_KNOWN_PEER
	case raft.ErrUnknownPeer: return C.RAFT_E_UNKNOWN_PEER
	case raft.ErrLogNotFound: return C.RAFT_E_LOG_NOT_FOUND
	case raft.ErrPipelineReplicationNotSupported: return C.RAFT_E_PIPELINE_REPLICATION_NOT_SUPP
	case raft.ErrTransportShutdown: return C.RAFT_E_TRANSPORT_SHUTDOWN
	case raft.ErrPipelineShutdown: return C.RAFT_E_PIPELINE_SHUTDOWN
	default: return C.RAFT_E_OTHER
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

	//fmt.Printf("[DEBUG] Applying command to C FSM: %q\n", log.Data)

	switch log.Type {
	case raft.LogCommand: c_type = C.RAFT_LOG_COMMAND
	case raft.LogNoop: c_type = C.RAFT_LOG_NOOP
	case raft.LogAddPeer: c_type = C.RAFT_LOG_ADD_PEER
	case raft.LogRemovePeer: c_type = C.RAFT_LOG_REMOVE_PEER
	case raft.LogBarrier: c_type = C.RAFT_LOG_BARRIER
	default:
		lg.Panicln("Unhandled log type!")
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
	lg.Println("=== FSM snapshot requested ===");
	return &MockSnapshot{m.logs, len(m.logs)}, nil
}

func (m *MockFSM) Restore(inp io.ReadCloser) error {
	m.Lock()
	defer m.Unlock()
	defer inp.Close()
	lg.Println("=== FSM restore requested ===");
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
