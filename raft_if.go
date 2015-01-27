package main

import (
	// #cgo CXXFLAGS: -std=c++11 -Wall -Werror -Wextra -Wconversion -pedantic
	// #cgo LDFLAGS: -lraft
	// #include "raft_go_if.h"
	"C"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-mdb"
	// should just be for scaffolding
	"github.com/hashicorp/go-msgpack/codec"
)

// lifted from http://bazaar.launchpad.net/~niemeyer/gommap/trunk/view/head:/gommap.go
type Shm []byte

var (

	ri *raft.Raft
	shm Shm
	lg *log.Logger

	ErrFileExists = errors.New("file exists")
	ErrSnapshot = errors.New("snapshot failed")
	ErrRestore = errors.New("restore failed")
)

type RaftServices struct {
	logs   raft.LogStore
	stable raft.StableStore
	snaps  raft.SnapshotStore
}

func main() {
	var err error

	pid  := os.Getpid()
	ppid := os.Getppid()
	lg = log.New(os.Stderr, fmt.Sprintf("raft_if [%d] ", pid), log.LstdFlags)

	dir := flag.String("dir", "", "State directory")
	port := flag.Int("port", 9001, "Raft port to listen on")
	single := flag.Bool("single", false, "Run in single-node mode")
	peers_s := flag.String("peers", "", "Static list of peers")
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
	lg.Printf("Single node: %v\n", conf.EnableSingleNode)
	fsm := &RemoteFSM{}

	var svcs *RaftServices
	var peers raft.PeerStore

	if *dir != "" {
		lg.Printf("Setting up standard Raft services in %s.\n", *dir)
		svcs, err = StdServices(*dir)
	} else {
		lg.Println("Setting up dummy Raft services.")
		svcs, err = DummyServices()
	}
	if err != nil {
		lg.Fatalf("Failed to initialize Raft base services: %v\n", err)
	}

	bindAddr := fmt.Sprintf("127.0.0.1:%d", *port)
	lg.Printf("Binding to %s.\n", bindAddr)
	trans, err := raft.NewTCPTransport(bindAddr, nil, 16, 0, nil)
	if err != nil {
		lg.Panicf("Binding to %s failed: %v", bindAddr, err)
	}

	if (*peers_s != "" || *dir == "") {
		lg.Printf("Setting up static peers: %s\n", *peers_s)
		peers, err = StaticPeers(*peers_s)
		if err != nil {
			lg.Fatalf("Failed to initialize peer set: %v\n", err)
		}
	} else {
		lg.Printf("Setting up JSON peers in %s.\n", *dir)
		peers = raft.NewJSONPeers(*dir, trans)
	}

	raft, err := raft.NewRaft(conf, fsm,
		svcs.logs, svcs.stable, svcs.snaps, peers, trans)
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

func DummyServices() (*RaftServices, error) {
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
	return &RaftServices{ logStore, stableStore, snapStore }, nil
}

func StdServices(base string) (*RaftServices, error) {
	var err error
	if err = MkdirIfNeeded(base); err != nil {
		return nil, err
	}
	snapDir := path.Join(base, "snapshots")
	if err = MkdirIfNeeded(snapDir); err != nil {
		return nil, err
	}

	mdbStore, err := raftmdb.NewMDBStore(base)
	if err != nil {
		lg.Printf("Creating MDBStore for %s failed: %v\n", base, err)
		return nil, err
	}
	// TODO: knobs for snapshot retention etc
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 1, os.Stderr)
	if err != nil {
		lg.Printf("Creating FileSnapshotStore for %s failed: %v\n",
			snapDir, err)
		return nil, err
	}
	return &RaftServices{ mdbStore, mdbStore, snapStore }, nil
}

func MkdirIfNeeded(path string) error {
	dir_info, err := os.Stat(path)
	if err == nil {
		if dir_info.IsDir() {
			// directory exists
			return nil
		} else {
			return ErrFileExists
		}
	} else if os.IsNotExist(err) {
		err = os.Mkdir(path, 0755)
		if err == nil {
			return nil
		} else {
			lg.Printf("Failed to create Raft dir %s: %v", path, err)
			return err
		}
	} else {
		// some other error?
		return err
	}
}

func StaticPeers(peers_s string) (raft.PeerStore, error) {
	peerL := strings.Split(peers_s, ",")
	peerAddrs := make([]net.Addr, 0, len(peerL))
	for i := range peerL {
		peer := peerL[i]
		if len(peer) == 0 { continue; }
		addr, err := net.ResolveTCPAddr("tcp", peer)
		if err != nil {
			lg.Panicf("Failed to parse address %s", peer)
		}
		
		peerAddrs = append(peerAddrs, addr)
	}
	lg.Printf("Static peers: %v\n", peerAddrs)
	return &raft.StaticPeers{ StaticPeers: peerAddrs }, nil
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

func SendReply(call C.raft_call, future raft.Future) {
	if future.Error() == nil {
		C.raft_reply(call, C.RAFT_SUCCESS)
	} else {
		lg.Printf("Command failed: %v\n", future.Error())
		C.raft_reply(call, TranslateRaftError(future.Error()))
	}
}

func SendApplyReply(call C.raft_call, future raft.ApplyFuture) {
	if future.Error() == nil {
		response := future.Response()
		C.raft_reply_apply(call, response.(C.uint64_t), C.RAFT_SUCCESS)
	} else {
		lg.Printf("Command failed: %v\n", future.Error())
		C.raft_reply_apply(call, 0, TranslateRaftError(future.Error()))
	}
}

// Interface functions

// note: multiple return value structs have members r0, r1, ...
// errors are represented as a GoInterface struct
// typedef struct { void *data; GoInt len; GoInt cap; } GoSlice
// typedef struct { char *p; GoInt n; } GoString

//export RaftApply
func RaftApply(call C.raft_call, cmd_offset uintptr, cmd_len uintptr, timeout uint64) {
	cmd := shm[cmd_offset:cmd_offset+cmd_len]
	//ri.logger.Printf("[INFO] Applying command (%d bytes): %q\n", len(cmd), cmd)
	future := ri.Apply(cmd, time.Duration(timeout))
	go SendApplyReply(call, future);
}

//export RaftSnapshot
func RaftSnapshot(call C.raft_call) {
	lg.Println("Received snapshot API request.")
	future := ri.Snapshot()
	go SendReply(call, future)
}

//export RaftAddPeer
func RaftAddPeer(call C.raft_call, host *C.char, port C.uint16_t) {
	host_s := C.GoString(host)
	addr_s := fmt.Sprintf("%s:%u", host_s, port)
	addr, err := net.ResolveTCPAddr("tcp", addr_s)
	if err != nil {
		future := ri.AddPeer(addr)
		go SendReply(call, future)
	} else {
		lg.Printf("Failed to resolve peer address %s: %v\n",
			addr_s, err)
		C.raft_reply(call, C.RAFT_E_OTHER)
	}
}

//export RaftRemovePeer
func RaftRemovePeer(call C.raft_call, host *C.char, port C.uint16_t) {
	host_s := C.GoString(host)
	addr_s := fmt.Sprintf("%s:%u", host_s, port)
	addr, err := net.ResolveTCPAddr("tcp", addr_s)
	if err != nil {
		future := ri.RemovePeer(addr)
		go SendReply(call, future)
	} else {
		lg.Printf("Failed to resolve peer address %s: %v\n",
			addr_s, err)
		C.raft_reply(call, C.RAFT_E_OTHER)
	}
}

//export RaftShutdown
func RaftShutdown(call C.raft_call) {
	lg.Println("Requesting Raft shutdown.")
	future := ri.Shutdown()
	lg.Println("Waiting for Raft to shut down...");
	go SendReply(call, future)
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
type RemoteFSM struct {
	sync.Mutex
	logs [][]byte
}

type MockSnapshot struct {
	logs     [][]byte
	maxIndex int
}

type PipeSnapshot struct {
	path     string
	complete chan bool
}

func (m *RemoteFSM) Apply(log *raft.Log) interface{} {
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

func (m *RemoteFSM) Snapshot() (raft.FSMSnapshot, error) {
	lg.Println("=== FSM snapshot requested ===");
	path, err := MakeFIFO()
	if err == nil {
		snap := PipeSnapshot{ path, make(chan bool) }
		go StartSnapshot(&snap)
		return &snap, nil
	} else {
		lg.Printf("Failed to create snapshot FIFO %s: %v", path, err)
		return nil, err
	} 
}

func (m *RemoteFSM) Restore(inp io.ReadCloser) error {
	defer inp.Close()
	lg.Println("=== FSM restore requested ===");
	path, err := MakeFIFO()
	if err != nil { return err }
	status := make(chan bool)
	go StartRestore(path, inp, status)
	fsm_result := C.raft_fsm_restore(C.CString(path))
	pipe_result := <- status
	if (fsm_result == 0) && pipe_result {
		lg.Println("Restore succeeded.")
		return nil
	} else {
		lg.Println("Restore failed.")
		return ErrRestore
	}
}

func MakeFIFO() (string, error) {
	random, err := rand.Int(rand.Reader, big.NewInt(1<<32))
	if err != nil {
		lg.Printf("Failed to obtain random number: %v\n", err)
		return "", err
	}
	path := fmt.Sprintf("/tmp/fsm_snap_%d", random)
	err = syscall.Mkfifo(path, 0600)
	if err == nil {
		return path, nil
	} else {
		lg.Printf("mkfifo failed for %s: %v\n", path, err)
		return "", err
	}
}

func StartSnapshot(snap *PipeSnapshot) {
	retval := C.raft_fsm_snapshot(C.CString(snap.path))
	success := (retval == 0)
	snap.complete <- success
}

func StartRestore(fifo string, source io.ReadCloser, status chan bool) {
	sink, err := os.OpenFile(fifo, os.O_WRONLY, 0000)
	if err != nil {
		lg.Printf("Failed to open FIFO %s: %v\n", fifo, err)
		status <- false
		return
	}
	defer sink.Close()
	err = os.Remove(fifo)
	if err != nil {
		lg.Printf("Failed to remove FIFO %s: %v\n", fifo, err)
		status <- false
		return
	}

	n_read, err := io.Copy(sink, source)
	if err == nil {
		if err = sink.Close(); err == nil {
			lg.Printf("Wrote snapshot for restore, %d bytes.\n", n_read)
			status <- true
			return
		}
	}
	status <- false
}

func (p *PipeSnapshot) Persist(sink raft.SnapshotSink) error {
	source, err := os.Open(p.path)
	if err != nil {
		// TODO: more cleanup?
		sink.Cancel()
		return err
	}
	defer source.Close()
	err = os.Remove(p.path)
	if err != nil {
		lg.Printf("Failed to remove FIFO: %v\n", err)
		return err
	}

	written, err := io.Copy(sink, source)
	if err == nil {
		switch <- p.complete {
		case true:
			sink.Close()
			lg.Printf("Snapshot succeeded, %d bytes.\n", written)
			return nil
		case false:
			sink.Cancel()
			return ErrSnapshot
		default:
			lg.Panicf("should never happen")
			return ErrSnapshot // not reached
		}
	} else {
		sink.Cancel()
		return err
	}
}

func (p *PipeSnapshot) Release() {
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
