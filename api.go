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
	"fmt"
	"github.com/hashicorp/raft"
	"net"
	"time"
)

// Interface functions

// note: multiple return value structs have members r0, r1, ...
// errors are represented as a GoInterface struct
// typedef struct { void *data; GoInt len; GoInt cap; } GoSlice
// typedef struct { char *p; GoInt n; } GoString

//export RaftApply
func RaftApply(call C.raft_call, cmd_offset uintptr, cmd_len uintptr,
	timeout_ns uint64) {
	cmd := shm[cmd_offset : cmd_offset+cmd_len]
	future := ri.Apply(cmd, time.Duration(timeout_ns))
	go SendApplyReply(call, future)
}

//export RaftBarrier
func RaftBarrier(call C.raft_call, timeout_ns uint64) {
	future := ri.Barrier(time.Duration(timeout_ns))
	go SendReply(call, future)
}

//export RaftVerifyLeader
func RaftVerifyLeader(call C.raft_call) {
	future := ri.VerifyLeader()
	go SendReply(call, future)
}

//export RaftGetState
func RaftGetState(call C.raft_call) {
	go SendReplyFrom(call,
		func () (error, uint64) {
			// weird name conflicts with using the actual enum
			switch ri.State().String() {
			case "Follower":
				return nil, C.RAFT_FOLLOWER
			case "Candidate":
				return nil, C.RAFT_CANDIDATE
			case "Leader":
				return nil, C.RAFT_LEADER
			case "Shutdown":
				return nil, C.RAFT_SHUTDOWN
			default:
				return nil, C.RAFT_INVALID_STATE
			}
		})
}

//export RaftLastContact
func RaftLastContact(call C.raft_call) {
	go SendReplyFrom(call,
		func () (error, uint64) {
			return nil, uint64(ri.LastContact().Unix())
		})
}

//export RaftLastIndex
func RaftLastIndex(call C.raft_call) {
	go SendReplyFrom(call,
		func () (error, uint64) {
			return nil, ri.LastIndex()
		})
}

//export RaftGetLeader
func RaftGetLeader(call C.raft_call) {
	go SendReplyFrom(call,
		func() (error, uint64) {
			leader := ri.Leader()
			if leader != nil {
				return nil, shmString(leader.String())
			} else {
				return raft.ErrUnknownPeer, 0
			}
		})
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
	addr_s := fmt.Sprintf("%s:%d", host_s, port)
	addr, err := net.ResolveTCPAddr("tcp", addr_s)
	if err == nil {
		future := ri.AddPeer(addr)
		go SendReply(call, future)
	} else {
		log.Error("Failed to resolve peer address %s: %v",
			addr_s, err)
		C.raft_reply_immed(call, C.RAFT_E_RESOLVE)
	}
}

//export RaftRemovePeer
func RaftRemovePeer(call C.raft_call, host *C.char, port C.uint16_t) {
	host_s := C.GoString(host)
	addr_s := fmt.Sprintf("%s:%d", host_s, port)
	addr, err := net.ResolveTCPAddr("tcp", addr_s)
	if err == nil {
		future := ri.RemovePeer(addr)
		go SendReply(call, future)
	} else {
		log.Error("Failed to resolve peer address %s: %v",
			addr_s, err)
		C.raft_reply_immed(call, C.RAFT_E_RESOLVE)
	}
}

//export RaftShutdown
func RaftShutdown(call C.raft_call) {
	log.Info("Requesting Raft shutdown.")
	future := ri.Shutdown()
	log.Debug("Waiting for Raft to shut down...")
	go SendReply(call, future)
}
