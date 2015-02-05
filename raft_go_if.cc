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

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <unistd.h>
#include <thread>
#include <vector>

#include "raft_go_if.h"
#include "raft_shm.h"
#include "queue.h"

extern "C" {
#include "_cgo_export.h"
}

using namespace raft;

using boost::interprocess::anonymous_instance;
using boost::interprocess::unique_instance;

namespace {

zlog_category_t*    go_cat;

void run_worker();
void take_call();

#define api_call(name, arg_t, has_ret) \
    void dispatch_ ## name(api::name::slot_t& slot);
#include "raft_api_calls.h"
#undef api_call

std::vector<std::thread> workers;

}

void raft_ready()
{
    uint32_t n_workers = raft_get_config()->api_workers;
    if (n_workers == 0) {
        zlog_warn(go_cat, "Must run more than 0 API workers, defaulting to 4.");
        n_workers = 4;
    }
    workers.reserve(n_workers);
    for (uint32_t i = 0; i < n_workers; ++i) {
        workers.emplace_back(run_worker);
    }
    
    raft::scoreboard->is_raft_running = true;
}

namespace {

void raft_reply_(BaseSlot& slot, RaftError error);

void run_worker()
{
    zlog_debug(go_cat, "Starting worker.");
    try {
        for (;;) {
            take_call();
        }
    } catch (queue::queue_closed&) {
        zlog_debug(go_cat, "API queue closed, Go worker exiting.");
        return;
    }
}

void take_call()
{
    auto rec = scoreboard->api_queue.take();
    auto recv_ts = Timings::clock::now();
    CallTag tag = rec.first;
    BaseSlot::pointer slot = rec.second;
    raft::mutex_lock l(slot->owned);
    slot->timings.record("call received", recv_ts);
    slot->timings.record("call locked");
    zlog_debug(go_cat, "API call received, tag %d, call %p.",
               tag, slot.get());
    assert(slot->state == raft::CallState::Pending);

    switch (tag) {
#define api_call(name, arg_t, has_ret)                  \
        case api::name::tag: \
            dispatch_ ## name((api::name::slot_t&) *slot); \
            break;
#include "raft_api_calls.h"
#undef api_call
    default:
        zlog_fatal(go_cat, "Unhandled call type: %d",
                   tag);
        abort();
    }
    //slot->state = raft::CallState::Dispatched;
}

uintptr_t shm_offset(void* ptr)
{
    uintptr_t shm_base = (uintptr_t) shm.get_address();
    uintptr_t shm_end = (uintptr_t) shm_base + shm.get_size();
    uintptr_t address = (uintptr_t) ptr;
    assert(address >= shm_base && address < shm_end);
    return address - shm_base;
}

void dispatch_Apply(api::Apply::slot_t& slot)
{
    assert(slot.tag == api::Apply::tag);
    
    size_t cmd_offset = shm_offset(slot.args.cmd_buf.get());

    RaftApply(&slot, cmd_offset, slot.args.cmd_len, slot.args.timeout_ns);
}

void dispatch_Barrier(api::Barrier::slot_t& slot)
{
    RaftBarrier(&slot, slot.args.timeout_ns);
}

void dispatch_VerifyLeader(api::VerifyLeader::slot_t& slot)
{
    RaftVerifyLeader(&slot);
}

void dispatch_GetState(api::GetState::slot_t& slot)
{
    RaftGetState(&slot);
}

void dispatch_LastContact(api::LastContact::slot_t& slot)
{
    RaftLastContact(&slot);
}

void dispatch_LastIndex(api::LastIndex::slot_t& slot)
{
    RaftLastIndex(&slot);
}

void dispatch_GetLeader(api::GetLeader::slot_t& slot)
{
    RaftGetLeader(&slot);
}

void dispatch_Snapshot(api::Snapshot::slot_t& slot)
{
    assert(slot.tag == CallTag::Snapshot);
    RaftSnapshot(&slot);
}

void dispatch_AddPeer(api::AddPeer::slot_t& slot)
{
    assert(slot.tag == CallTag::AddPeer);
    RaftAddPeer(&slot, (char*) slot.args.host, slot.args.port);
}

void dispatch_RemovePeer(api::RemovePeer::slot_t& slot)
{
    assert(slot.tag == CallTag::RemovePeer);
    RaftRemovePeer(&slot, (char*) slot.args.host, slot.args.port);
}

void dispatch_Shutdown(api::Shutdown::slot_t& slot)
{
    assert(slot.tag == CallTag::Shutdown);
    slot.state = raft::CallState::Dispatched;
    RaftShutdown(&slot);
}

}

void raft_reply(raft_call call_p, RaftError error)
{
    auto* slot = (BaseSlot*) call_p;
    mutex_lock lock(slot->owned);
    raft_reply_(*slot, error);
}

void raft_reply_immed(raft_call call_p, RaftError error)
{
    raft_reply_(*(BaseSlot*) call_p, error);
}

void raft_reply_apply(raft_call call_p, uint64_t retval, RaftError error)
{
    auto* slot = (BaseSlot*) call_p;
    mutex_lock lock(slot->owned);
    slot->timings.record("RaftApply return");
    assert(slot->tag == CallTag::Apply);
    if (!error) {
        slot->reply(retval);
    } else {
        zlog_error(go_cat, "Sending error response from RaftApply: %d", error);
        slot->reply(error);
    }
}

namespace {

/**
 * Send reply IFF we already hold the lock on slot!
 */
void raft_reply_(BaseSlot& slot, RaftError error)
{
    slot.timings.record("Raft call return");
    slot.reply(error);
}

}

void raft_reply_value(raft_call call, uint64_t retval)
{
    // TODO: check that this has a return value...
    auto* slot = (BaseSlot*) call;
    mutex_lock lock(slot->owned);
    slot->timings.record("Raft call return");
    slot->reply(retval);
}

uint64_t raft_fsm_apply(uint64_t index, uint64_t term, RaftLogType type,
                        char* cmd_buf, size_t cmd_len)
{
    shm_handle cmd_handle;
    char* shm_buf = nullptr;

    if (in_shm_bounds(cmd_buf)) {
        cmd_handle = raft::shm.get_handle_from_address(cmd_buf);
    } else {
        shm_buf = (char*) raft::shm.allocate(cmd_len);
        assert(shm_buf);
        zlog_debug(go_cat, "Allocated %lu-byte buffer for log command at %p.",
                   cmd_len, shm_buf);
        memcpy(shm_buf, cmd_buf, cmd_len);
        cmd_handle = raft::shm.get_handle_from_address(shm_buf);
    }

    auto* slot =
        send_fsm_request<api::FSMApply>(index, term, type, cmd_handle, cmd_len);
    slot->wait();

    assert(slot->state == raft::CallState::Success);
    assert(slot->error == RAFT_SUCCESS);
    zlog_debug(go_cat, "FSM response %#" PRIx64 , slot->retval);

    if (shm_buf)
        shm.deallocate(shm_buf);

    //slot->timings.print();
    //fprintf(stderr, "====================\n");

    auto rv = slot->retval;
    slot->dispose();

    return rv;
}

int raft_fsm_snapshot(char *path)
{
    auto* slot = send_fsm_request<api::FSMSnapshot>(path);
    free(path);
    slot->wait();

    assert(is_terminal(slot->state));
    int retval = (slot->state == raft::CallState::Success) ? 0 : 1;
    slot->dispose();
    return retval;
}

int raft_fsm_restore(char *path)
{
    auto* slot = send_fsm_request<api::FSMRestore>(path);
    free(path);
    zlog_info(go_cat, "Sent restore request to FSM.");
    slot->wait();

    assert(is_terminal(slot->state));
    int retval = (slot->state == raft::CallState::Success) ? 0 : 1;
    slot->dispose();
    return retval;
}

void raft_set_leader(bool val)
{
    zlog_info(go_cat, "Leadership state change: %s",
              val ? "is leader" : "not leader");
    raft::scoreboard->is_leader = val;
}

void* raft_shm_init(const char *shm_path)
{
    raft::shm_init(shm_path, false, nullptr);
    free((void*) shm_path);
    go_cat = zlog_get_category("raft_go");
    return raft::shm.get_address();
}

size_t raft_shm_size()
{
    return raft::shm.get_size();
}

uint64_t raft_shm_string(const char *str, size_t len)
{
    char* buf = (char*) raft::shm.allocate(len+1);
    assert(raft::in_shm_bounds((void*) buf));
    memcpy(buf, str, len);
    free((void*) str);
    buf[len] = '\0';
    return (uint64_t) raft::shm.get_handle_from_address(buf);
}

RaftConfig* raft_get_config()
{
    return raft::shm.find<RaftConfig>(unique_instance).first;
}
