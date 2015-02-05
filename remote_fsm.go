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
	// #include "raft_go_if.h"
	"C"
	"crypto/rand"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"math/big"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

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

	rv := C.raft_fsm_apply(C.uint64_t(rlog.Index), C.uint64_t(rlog.Term), c_type, cmd_buf, cmd_len)

	return rv
}

func (m *RemoteFSM) Snapshot() (raft.FSMSnapshot, error) {
	log.Debug("=== FSM snapshot requested ===")
	path, err := MakeFIFO()
	if err == nil {
		snap := PipeSnapshot{path, make(chan bool)}
		go StartSnapshot(&snap)
		return &snap, nil
	} else {
		log.Error("Failed to create snapshot FIFO %s: %v", path, err)
		return nil, err
	}
}

func (m *RemoteFSM) Restore(inp io.ReadCloser) error {
	defer inp.Close()
	log.Debug("=== FSM restore requested ===")
	path, err := MakeFIFO()
	if err != nil {
		return err
	}
	status := make(chan bool)
	go StartRestore(path, inp, status)
	log.Debug("Sending restore request to FSM.")
	fsm_result := C.raft_fsm_restore(C.CString(path))
	pipe_result := <-status
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
		switch <-p.complete {
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
