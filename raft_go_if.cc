#include <chrono>
#include <cstdio>
#include <unistd.h>
#include <thread>
#include <vector>

#include "raft_go_if.h"
#include "raft_shm.h"

extern "C" {
#include "_cgo_export.h"
}

using mutex_lock = raft::mutex_lock;
using boost::interprocess::anonymous_instance;

namespace {

void dispatch_apply(raft::RaftCallSlot& slot, raft::ApplyCall& call);
void run_worker(uint32_t slot_n);
void await_call(uint32_t slot_n);
RaftError translate_raft_error(GoInterface& err_if);

const static uint32_t N_WORKERS = 16;

std::vector<std::thread> workers;

}

void raft_ready()
{
    workers.reserve(N_WORKERS);
    for (uint32_t i = 0; i < N_WORKERS; ++i) {
        workers.emplace_back(run_worker, i);
    }
    
    raft::scoreboard->is_raft_running = true;
}

namespace {

void run_worker(uint32_t slot_n)
{
    fprintf(stderr, "Starting worker %u.\n", slot_n);
    for (;;) {
        await_call(slot_n);
    }
}

void await_call(uint32_t slot_n)
{
    assert(slot_n < 16);
    raft::RaftCallSlot& slot = raft::scoreboard->slots[slot_n];
    mutex_lock l(slot.owned);
    slot.call_cond.wait(l, [&] () { return slot.call_ready; });
    slot.timings.record("call received");

    assert(slot.state == raft::CallState::Pending);

    switch (slot.call_type) {
    case raft::APICall::Apply:
        dispatch_apply(slot, slot.call.apply);
        break;
    }

    slot.call_ready = false;
    slot.timings.record("return issued");
}

void dispatch_apply(raft::RaftCallSlot& slot, raft::ApplyCall& call)
{
    assert(slot.call_type == raft::APICall::Apply);
    
    void* cmd_ptr = raft::shm.get_address_from_handle(call.cmd_buf);
    fprintf(stderr, "Found command buffer at %p.\n", cmd_ptr);
    void* shm_ptr = raft::shm.get_address();
    assert(cmd_ptr > shm_ptr);
    size_t cmd_offset = ((char*) cmd_ptr) - ((char*) shm_ptr);

    slot.timings.record("RaftApply call");
    auto res = RaftApply(cmd_offset, call.cmd_len, call.timeout_ns);
    slot.timings.record("RaftApply return");
    slot.error = res.r1;
    if (!slot.error) {
        slot.state = raft::CallState::Success;
        slot.retval = (uintptr_t) res.r0;
    } else {
        slot.state = raft::CallState::Error;
    }
    slot.ret_ready = true;
    slot.ret_cond.notify_one();
}

}

uint64_t raft_fsm_apply(uint64_t index, uint64_t term, RaftLogType type,
                        char* cmd_buf, size_t cmd_len)
{
    auto start_t = std::chrono::high_resolution_clock::now();
    auto& slot = raft::scoreboard->fsm_slot;
    raft::SlotHandle<raft::FSMOp> sh(slot);
    raft::mutex_lock l(slot.owned);
    assert(slot.call_ready == false);

    slot.call_type = raft::FSMOp::Apply;
    slot.state = raft::CallState::Pending;

    raft::LogEntry& log = slot.call.log_entry; 
    log.index = index;
    log.term = term;
    log.log_type = type;

    char *shm_buf = nullptr;
    
    if (raft::in_shm_bounds(cmd_buf)) {
        log.data_buf = raft::shm.get_handle_from_address(cmd_buf);
        assert(log.data_buf);
    } else {
        shm_buf = (char*) raft::shm.allocate(cmd_len);
        assert(shm_buf);
        fprintf(stderr, "Allocated %lu-byte buffer for log command at %p.\n",
                cmd_len, shm_buf);
        memcpy(shm_buf, cmd_buf, cmd_len);
        log.data_buf = raft::shm.get_handle_from_address(shm_buf);
    }
    log.data_len = cmd_len;

    slot.call_ready = true;
    slot.call_cond.notify_one();
    slot.ret_cond.wait(l, [&] () { return slot.ret_ready; });

    assert(sh.slot.state == raft::CallState::Success);

    slot.ret_ready = false;
    slot.call_ready = false;
    if (shm_buf)
        raft::shm.deallocate(shm_buf);

    auto end_t = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end_t - start_t);
    fprintf(stderr, "FSM Apply call completed in %lld us.\n", elapsed.count());

    return slot.retval;
}

void raft_set_leader(bool val)
{
    fprintf(stderr, "Leadership state change: %s\n",
            val ? "is leader" : "not leader");
    raft::scoreboard->is_leader = val;
}

void* raft_shm_init()
{
    raft::shm_init("raft", false);
    return raft::shm.get_address();
}

size_t raft_shm_size()
{
    return raft::shm.get_size();
}
