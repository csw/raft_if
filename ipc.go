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
	"github.com/hashicorp/raft"
	"reflect"
	"syscall"
	"time"
	"unsafe"
)

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
		if err != nil {
			log.Error("Parent process %d has exited!", ppid)
			OnParentExit()
		}
		time.Sleep(1 * time.Second)
	}
}

func OnParentExit() {
	future := ri.Shutdown()
	log.Info("Waiting for Raft to shut down...")
	if future.Error() != nil {
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
		SendErrorReply(call, future.Error())
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

func SendReplyFrom(call C.raft_call, fun func () (error, uint64)) {
	err, value := fun()
	if err == nil {
		log.Debug("Sending success reply, value=%d / %#x", value, value)
		C.raft_reply_value(call, C.uint64_t(value))
	} else {
		SendErrorReply(call, err)
	}
}

func SendErrorReply(call C.raft_call, err error) {
	log.Warning("Command failed: %v", err)
	C.raft_reply(call, TranslateRaftError(err))
}

func shmString(s string) uint64 {
	// double copy is inefficient but safest
	cs := C.CString(s)
	return uint64(C.raft_shm_string(cs, C.size_t(len(s))))
}

func shmBuf(buf []byte) uint64 {
	// XXX: check that this is OK
	cs := C.CString(string(buf))
	return uint64(C.raft_shm_string(cs, C.size_t(len(buf)+1)))
}
