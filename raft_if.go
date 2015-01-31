package main

import (
	// #cgo CXXFLAGS: -std=c++11
	// #cgo CXXFLAGS: -Wall -Werror -Wextra
	// #cgo CXXFLAGS: -Wconversion -Wno-variadic-macros
	// #cgo CXXFLAGS: -Wno-gnu-zero-variadic-macro-arguments
	// #include "raft_go_if.h"
	"C"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"reflect"
	"strings"
	"syscall"
	"time"
	"unsafe"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-mdb"
	"github.com/op/go-logging"
)

// lifted from http://bazaar.launchpad.net/~niemeyer/gommap/trunk/view/head:/gommap.go
type Shm []byte

var (

	ri *raft.Raft
	shm Shm
	log *logging.Logger

	logFormat = "%{color}%{time:2006-01-02 15:04:05} %{level:.5s} %{module} [%{pid}]%{color:reset} %{message}"

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

	ppid := os.Getppid()
	
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	logging.SetBackend(backend)
	logging.SetFormatter(logging.MustStringFormatter(logFormat))
	log = logging.MustGetLogger("raft_if")
	logging.SetLevel(logging.INFO, "raft_if")
	
	shm_path := flag.String("shm", "/tmp/raft_shm", "Shared memory")
	flag.Parse()

	log.Info("Starting Raft service for parent PID %d.", ppid)
	log.Debug("Initializing Raft shared memory.")

	nshm, err := ShmInit(*shm_path)
	if (err != nil) {
		log.Panic("Failed to initialize shared memory!")
	}
	shm = nshm
	log.Debug("Shared memory initialized.")
	go WatchParent(ppid)

	shared_conf := C.raft_get_config()
	conf  := CopyConfig(shared_conf)
	dir   := C.GoString(&shared_conf.base_dir[0])
	port  := uint16(shared_conf.listen_port)
	peers_s := C.GoString(&shared_conf.peers[0])

	fsm := &RemoteFSM{}

	var svcs *RaftServices
	var peers raft.PeerStore

	if dir != "" {
		log.Info("Setting up standard Raft services in %s.", dir)
		svcs, err = StdServices(dir)
	} else {
		log.Info("Setting up dummy Raft services.")
		svcs, err = DummyServices()
	}
	if err != nil {
		log.Fatalf("Failed to initialize Raft base services: %v", err)
	}

	bindAddr := fmt.Sprintf("127.0.0.1:%d", port)
	log.Info("Binding to %s.", bindAddr)
	trans, err := raft.NewTCPTransport(bindAddr, nil, 16, 0, nil)
	if err != nil {
		log.Panicf("Binding to %s failed: %v", bindAddr, err)
	}

	if (peers_s != "" || dir == "") {
		log.Info("Setting up static peers: %s", peers_s)
		peers, err = StaticPeers(peers_s)
		if err != nil {
			log.Fatalf("Failed to initialize peer set: %v", err)
		}
	} else {
		log.Info("Setting up JSON peers in %s.", dir)
		peers = raft.NewJSONPeers(dir, trans)
	}

	raft, err := raft.NewRaft(conf, fsm,
		svcs.logs, svcs.stable, svcs.snaps, peers, trans)
	if (err != nil) {
		log.Panicf("Failed to create Raft instance: %v", err)
	}

	ri = raft
	if StartWorkers() != nil {
		log.Panicf("Failed to start workers: %v", err)
	}

	C.raft_ready()
	log.Info("Raft is ready.")

	for raft.State().String() != "Shutdown" {
		time.Sleep(1*time.Second)
	}
	// XXX: race with shutdown handler thread etc.
	time.Sleep(2*time.Second)
	log.Info("raft_if exiting.")
}

func CopyConfig(uc *C.RaftConfig) *raft.Config {
	rc := raft.DefaultConfig()

	rc.HeartbeatTimeout = time.Duration(uc.HeartbeatTimeout)
	rc.ElectionTimeout = time.Duration(uc.ElectionTimeout)
	rc.CommitTimeout = time.Duration(uc.CommitTimeout)

	rc.MaxAppendEntries = int(uc.MaxAppendEntries)
	rc.ShutdownOnRemove = bool(uc.ShutdownOnRemove)
	rc.DisableBootstrapAfterElect = bool(uc.DisableBootstrapAfterElect)
	rc.TrailingLogs = uint64(uc.TrailingLogs)
	rc.SnapshotInterval = time.Duration(uc.SnapshotInterval)
	rc.SnapshotThreshold = uint64(uc.SnapshotThreshold)
	rc.EnableSingleNode = bool(uc.EnableSingleNode)
	rc.LeaderLeaseTimeout = time.Duration(uc.LeaderLeaseTimeout)

	//logOutput := C.GoString(&uc.LogOutput[0])
	// TODO: set this up appropriately
	return rc
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
		log.Panicf("Creating snapshot store in %s failed: %v",
			snapDir, err)
	}
	return &RaftServices{ logStore, stableStore, snapStore }, nil
}

func StdServices(base string) (*RaftServices, error) {
	var err error
	if err = MkdirIfNeeded(base); err != nil {
		return nil, err
	}

	mdbStore, err := raftmdb.NewMDBStore(base)
	if err != nil {
		log.Error("Creating MDBStore for %s failed: %v\n", base, err)
		return nil, err
	}
	// TODO: knobs for snapshot retention etc
	snapStore, err := raft.NewFileSnapshotStore(base, 1, os.Stderr)
	if err != nil {
		log.Error("Creating FileSnapshotStore for %s failed: %v\n",
			base, err)
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
			log.Error("Failed to create Raft dir %s: %v", path, err)
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
			log.Panicf("Failed to parse address %s", peer)
		}
		
		peerAddrs = append(peerAddrs, addr)
	}
	log.Debug("Static peers: %v", peerAddrs)
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

func ShmInit(path string) (Shm, error) {
	shared_base := C.raft_shm_init(C.CString(path))
	if shared_base == nil {
		log.Panicf("Failed to allocate shared memory!")
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
			log.Error("Parent process %d has exited!", ppid)
			OnParentExit()
		}
		time.Sleep(1*time.Second)
	}
}

func OnParentExit() {
	future := ri.Shutdown()
	log.Info("Waiting for Raft to shut down...");
	if (future.Error() != nil) {
		log.Fatalf("Error during shutdown: %v", future.Error())
	} else {
		log.Fatal("Shutdown due to parent exit complete.")
	}
}

func SendReply(call C.raft_call, future raft.Future) {
	if future.Error() == nil {
		log.Debug("Sending success reply.")
		C.raft_reply(call, C.RAFT_SUCCESS)
	} else {
		log.Warning("Command failed: %v", future.Error())
		C.raft_reply(call, TranslateRaftError(future.Error()))
	}
}

func SendApplyReply(call C.raft_call, future raft.ApplyFuture) {
	if future.Error() == nil {
		response := future.Response()
		C.raft_reply_apply(call, response.(C.uint64_t), C.RAFT_SUCCESS)
	} else {
		log.Warning("Command failed: %v", future.Error())
		C.raft_reply_apply(call, 0, TranslateRaftError(future.Error()))
	}
}

// Interface functions

// note: multiple return value structs have members r0, r1, ...
// errors are represented as a GoInterface struct
// typedef struct { void *data; GoInt len; GoInt cap; } GoSlice
// typedef struct { char *p; GoInt n; } GoString

//export RaftApply
func RaftApply(call C.raft_call, cmd_offset uintptr, cmd_len uintptr,
	timeout_ns uint64) {
	cmd := shm[cmd_offset:cmd_offset+cmd_len]
	future := ri.Apply(cmd, time.Duration(timeout_ns))
	go SendApplyReply(call, future);
}

//export RaftBarrier
func RaftBarrier(call C.raft_call, timeout_ns uint64) {
	future := ri.Barrier(time.Duration(timeout_ns))
	go SendReply(call, future);
}

//export RaftVerifyLeader
func RaftVerifyLeader(call C.raft_call) {
	future := ri.VerifyLeader()
	go SendReply(call, future);
}

//export RaftSnapshot
func RaftSnapshot(call C.raft_call) {
	log.Debug("Received snapshot API request.")
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
		log.Error("Failed to resolve peer address %s: %v",
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
		log.Error("Failed to resolve peer address %s: %v",
			addr_s, err)
		C.raft_reply(call, C.RAFT_E_OTHER)
	}
}

//export RaftShutdown
func RaftShutdown(call C.raft_call) {
	log.Info("Requesting Raft shutdown.")
	future := ri.Shutdown()
	log.Debug("Waiting for Raft to shut down...");
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

type RemoteFSM struct {
}

type PipeSnapshot struct {
	path     string
	complete chan bool
}

func (m *RemoteFSM) Apply(rlog *raft.Log) interface{} {
	var c_type C.RaftLogType
	var cmd_buf *C.char
	var cmd_len C.size_t

	//fmt.Printf("[DEBUG] Applying command to C FSM: %q\n", log.Data)

	switch rlog.Type {
	case raft.LogCommand:    c_type = C.RAFT_LOG_COMMAND
	case raft.LogNoop:       c_type = C.RAFT_LOG_NOOP
	case raft.LogAddPeer:    c_type = C.RAFT_LOG_ADD_PEER
	case raft.LogRemovePeer: c_type = C.RAFT_LOG_REMOVE_PEER
	case raft.LogBarrier:    c_type = C.RAFT_LOG_BARRIER
	default:
		log.Panic("Unhandled log type!")
	}
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&rlog.Data))
	cmd_buf = (*C.char)(unsafe.Pointer(dh.Data))
	cmd_len = C.size_t(dh.Len)
	
	rv := C.raft_fsm_apply(C.uint64_t(rlog.Index), C.uint64_t(rlog.Term), c_type, cmd_buf, cmd_len);

	return rv
}

func (m *RemoteFSM) Snapshot() (raft.FSMSnapshot, error) {
	log.Debug("=== FSM snapshot requested ===");
	path, err := MakeFIFO()
	if err == nil {
		snap := PipeSnapshot{ path, make(chan bool) }
		go StartSnapshot(&snap)
		return &snap, nil
	} else {
		log.Error("Failed to create snapshot FIFO %s: %v", path, err)
		return nil, err
	} 
}

func (m *RemoteFSM) Restore(inp io.ReadCloser) error {
	defer inp.Close()
	log.Debug("=== FSM restore requested ===");
	path, err := MakeFIFO()
	if err != nil { return err }
	status := make(chan bool)
	go StartRestore(path, inp, status)
	log.Debug("Sending restore request to FSM.")
	fsm_result := C.raft_fsm_restore(C.CString(path))
	pipe_result := <- status
	if (fsm_result == 0) && pipe_result {
		log.Info("Restore succeeded.")
		return nil
	} else {
		log.Error("Restore failed.")
		return ErrRestore
	}
}

func MakeFIFO() (string, error) {
	random, err := rand.Int(rand.Reader, big.NewInt(1<<32))
	if err != nil {
		log.Error("Failed to obtain random number: %v", err)
		return "", err
	}
	path := fmt.Sprintf("/tmp/fsm_snap_%d", random)
	err = syscall.Mkfifo(path, 0600)
	if err == nil {
		return path, nil
	} else {
		log.Error("mkfifo failed for %s: %v", path, err)
		return "", err
	}
}

func StartSnapshot(snap *PipeSnapshot) {
	retval := C.raft_fsm_snapshot(C.CString(snap.path))
	success := (retval == 0)
	snap.complete <- success
}

func StartRestore(fifo string, source io.ReadCloser, status chan bool) {
	log.Debug("Opening FIFO %s for restore...", fifo)
	sink, err := os.OpenFile(fifo, os.O_WRONLY, 0000)
	if err != nil {
		log.Error("Failed to open FIFO %s: %v", fifo, err)
		status <- false
		return
	}
	defer sink.Close()
	err = os.Remove(fifo)
	if err != nil {
		log.Debug("Failed to remove FIFO %s: %v", fifo, err)
		status <- false
		return
	}

	n_read, err := io.Copy(sink, source)
	if err == nil {
		if err = sink.Close(); err == nil {
			log.Debug("Wrote snapshot for restore, %d bytes.", n_read)
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
		log.Error("Failed to remove FIFO: %v", err)
		return err
	}

	written, err := io.Copy(sink, source)
	if err == nil {
		switch <- p.complete {
		case true:
			sink.Close()
			log.Debug("Snapshot succeeded, %d bytes.", written)
			return nil
		case false:
			sink.Cancel()
			return ErrSnapshot
		default:
			log.Panicf("should never happen")
			return ErrSnapshot // not reached
		}
	} else {
		sink.Cancel()
		return err
	}
}

func (p *PipeSnapshot) Release() {
}
