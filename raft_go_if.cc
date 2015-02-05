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

#include <cinttypes>
#include <cstdio>
#include <unistd.h>
#include <thread>
#include <vector>

#include "raft_go_if.h"
#include "dispatch.h"
#include "raft_shm.h"
#include "queue.h"

using namespace raft;

using boost::interprocess::unique_instance;

zlog_category_t* go_cat;

namespace {

std::vector<std::thread> workers;

void raft_reply_(BaseSlot& slot, RaftError error);

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
        workers.push_back(dispatch::start_worker());
    }
    
    raft::scoreboard->is_raft_running = true;
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
