#include <cstdio>
#include <unistd.h>
#include <thread>

#include "raft_go_if.h"
#include "raft_shm.h"
#include "raft_client.h"

extern "C" {
#include "_cgo_export.h"
}

using mutex_lock = raft::mutex_lock;
using boost::interprocess::anonymous_instance;

void dispatch_apply(raft::RaftCallSlot& slot, raft::ApplyCall& call);

void raft_ready()
{
    raft::scoreboard->is_raft_running = true;
}

void await_call(uint32_t slot_n)
{
    assert(slot_n < 16);
    raft::RaftCallSlot& slot = raft::scoreboard->slots[slot_n];
    mutex_lock l(slot.owned);
    slot.call_cond.wait(l, [&] () { return slot.call_ready; });

    assert(slot.state == raft::CallState::Pending);
    void* call_addr = raft::shm.get_address_from_handle(slot.handle);
    assert(call_addr);
    fprintf(stderr, "Found call buffer at %p.\n", call_addr);

    switch (slot.call_type) {
    case raft::APICall::Apply:
        dispatch_apply(slot, *((raft::ApplyCall*)call_addr));
        break;
    }

    slot.call_ready = false;
}

void dispatch_apply(raft::RaftCallSlot& slot, raft::ApplyCall& call)
{
    assert(slot.call_type == raft::APICall::Apply);
    
    void* cmd_ptr = raft::shm.get_address_from_handle(call.cmd_buf);
    fprintf(stderr, "Found command buffer at %p.\n", cmd_ptr);
    void* shm_ptr = raft::shm.get_address();
    assert(cmd_ptr > shm_ptr);
    size_t cmd_offset = ((char*) cmd_ptr) - ((char*) shm_ptr);

    auto res = RaftApply(cmd_offset, call.cmd_len, call.timeout_ns);
    // XXX: dummy
    slot.state = raft::CallState::Success;
    slot.ret_ready = true;
    slot.ret_cond.notify_one();
}

uint64_t raft_fsm_apply(uint64_t index, uint64_t term, RaftLogType type,
                        char* cmd_buf, size_t cmd_len)
{
    auto& slot = raft::scoreboard->fsm_slot;
    raft::SlotHandle<raft::FSMOp> sh(slot);
    raft::mutex_lock l(slot.owned);
    assert(slot.call_ready == false);

    slot.call_type = raft::FSMOp::Apply;
    slot.state = raft::CallState::Pending;

    raft::LogEntry& log =
        *raft::shm.construct<raft::LogEntry>(anonymous_instance)();
    fprintf(stderr, "Allocated log entry at %p.\n", &log);
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
    slot.handle = raft::shm.get_handle_from_address(&log);

    slot.call_ready = true;
    slot.call_cond.notify_one();
    slot.ret_cond.wait(l, [&] () { return slot.ret_ready; });

    assert(sh.slot.state == raft::CallState::Success);

    slot.ret_ready = false;
    slot.call_ready = false;
    raft::shm.destroy_ptr(&log);
    if (shm_buf)
        raft::shm.deallocate(shm_buf);

    return slot.retval;
}

void raft_set_leader(bool val)
{
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
