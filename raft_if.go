package main

/*
 * raft_if, Go layer of libraft
 * Copyright (C) 2015 Clayton Wheeler
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 *
 */

import (
	// #cgo CXXFLAGS: -std=c++11
	// #cgo CXXFLAGS: -Wall -Werror -Wextra
	// #cgo CXXFLAGS: -Wconversion -Wno-variadic-macros
	// #cgo CXXFLAGS: -Wno-gnu-zero-variadic-macro-arguments
	// #include "raft_go_if.h"
	"C"
	"errors"
	"flag"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-mdb"
	"github.com/op/go-logging"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"
)

// lifted from http://bazaar.launchpad.net/~niemeyer/gommap/trunk/view/head:/gommap.go
type Shm []byte

var (
	ri  *raft.Raft
	shm Shm
	log *logging.Logger

	logFormat = "%{color}%{time:2006-01-02 15:04:05} %{level:.8s} %{module} [%{pid}]%{color:reset} %{message}"

	ErrFileExists = errors.New("file exists")
	ErrSnapshot   = errors.New("snapshot failed")
	ErrRestore    = errors.New("restore failed")
)

type RaftServices struct {
	logs   raft.LogStore
	stable raft.StableStore
	snaps  raft.SnapshotStore
}

func main() {
	shmPath := flag.String("shm-path", "/tmp/raft_shm", "Shared memory path")
	flag.Parse()
	Start(*shmPath)
}

func Start(shmPath string) {
	var err error

	ppid := os.Getppid()

	backend := logging.NewLogBackend(os.Stderr, "", 0)
	logging.SetBackend(backend)
	logging.SetFormatter(logging.MustStringFormatter(logFormat))
	log = logging.MustGetLogger("raft_if")
	logging.SetLevel(logging.INFO, "raft_if")

	log.Info("Starting Raft service for parent PID %d.", ppid)
	log.Debug("Initializing Raft shared memory.")

	nshm, err := ShmInit(shmPath)
	if err != nil {
		log.Panic("Failed to initialize shared memory!")
	}
	shm = nshm
	log.Debug("Shared memory initialized.")
	go WatchParent(ppid)

	shared_conf := C.raft_get_config()
	conf := CopyConfig(shared_conf)
	dir := C.GoString(&shared_conf.base_dir[0])
	port := uint16(shared_conf.listen_port)
	peers_s := C.GoString(&shared_conf.peers[0])

	fsm := &RemoteFSM{}

	var svcs *RaftServices
	var peers raft.PeerStore

	if dir != "" {
		log.Info("Setting up standard Raft services in %s.", dir)
		svcs, err = StdServices(dir, shared_conf)
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

	if peers_s != "" || dir == "" {
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
	if err != nil {
		log.Panicf("Failed to create Raft instance: %v", err)
	}

	ri = raft
	if StartWorkers() != nil {
		log.Panicf("Failed to start workers: %v", err)
	}

	C.raft_ready()
	log.Info("Raft is ready.")

	for raft.State().String() != "Shutdown" {
		time.Sleep(1 * time.Second)
	}
	// XXX: race with shutdown handler thread etc.
	time.Sleep(2 * time.Second)
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
	if err != nil {
		log.Panicf("Creating snapshot store in %s failed: %v",
			snapDir, err)
	}
	return &RaftServices{logStore, stableStore, snapStore}, nil
}

func StdServices(base string, cfg *C.RaftConfig) (*RaftServices, error) {
	var err error
	if err = MkdirIfNeeded(base); err != nil {
		return nil, err
	}

	mdbStore, err := raftmdb.NewMDBStore(base)
	if err != nil {
		log.Error("Creating MDBStore for %s failed: %v\n", base, err)
		return nil, err
	}
	// TODO: set log destination
	snapStore, err :=
		raft.NewFileSnapshotStore(base, int(cfg.RetainSnapshots), os.Stderr)
	if err != nil {
		log.Error("Creating FileSnapshotStore for %s failed: %v\n",
			base, err)
		return nil, err
	}
	return &RaftServices{mdbStore, mdbStore, snapStore}, nil
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
		if len(peer) == 0 {
			continue
		}
		addr, err := net.ResolveTCPAddr("tcp", peer)
		if err != nil {
			log.Panicf("Failed to parse address %s", peer)
		}

		peerAddrs = append(peerAddrs, addr)
	}
	log.Debug("Static peers: %v", peerAddrs)
	return &raft.StaticPeers{StaticPeers: peerAddrs}, nil
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

//export TranslateRaftError
func TranslateRaftError(err error) C.RaftError {
	switch err {
	case nil:
		return C.RAFT_SUCCESS
	case raft.ErrLeader:
		return C.RAFT_E_LEADER
	case raft.ErrNotLeader:
		return C.RAFT_E_NOT_LEADER
	case raft.ErrLeadershipLost:
		return C.RAFT_E_LEADERSHIP_LOST
	case raft.ErrRaftShutdown:
		return C.RAFT_E_SHUTDOWN
	case raft.ErrEnqueueTimeout:
		return C.RAFT_E_ENQUEUE_TIMEOUT
	case raft.ErrKnownPeer:
		return C.RAFT_E_KNOWN_PEER
	case raft.ErrUnknownPeer:
		return C.RAFT_E_UNKNOWN_PEER
	case raft.ErrLogNotFound:
		return C.RAFT_E_LOG_NOT_FOUND
	case raft.ErrPipelineReplicationNotSupported:
		return C.RAFT_E_PIPELINE_REPLICATION_NOT_SUPP
	case raft.ErrTransportShutdown:
		return C.RAFT_E_TRANSPORT_SHUTDOWN
	case raft.ErrPipelineShutdown:
		return C.RAFT_E_PIPELINE_SHUTDOWN
	default:
		return C.RAFT_E_OTHER
	}
}
