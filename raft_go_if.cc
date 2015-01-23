#include <cstdio>
#include <unistd.h>
#include <thread>

#include "raft_go_if.h"
#include "raft_shm.h"
#include "raft_client.h"

extern "C" {
#include "_cgo_export.h"
}

using mutex_lock = std::unique_lock<interprocess_mutex>;

void dispatch_apply(raft::CallSlot& slot, raft::ApplyCall& call);

void await_call(uint32_t slot_n)
{
    assert(slot_n < 16);
    raft::CallSlot& slot = raft::scoreboard->slots[slot_n];
    mutex_lock l(slot.owned);
    slot.call_cond.wait(l, [&] () { return slot.call_ready; });

    assert(slot.state == raft::CallState::Pending);
    void* call_addr = raft::shm.get_address_from_handle(slot.handle);
    assert(call_addr);
    fprintf(stderr, "Found call buffer at %p.\n", call_addr);

    switch (slot.call_type) {
    case raft::CallType::Apply:
        dispatch_apply(slot, *((raft::ApplyCall*)call_addr));
        break;
    }

    slot.call_ready = false;
}

void dispatch_apply(raft::CallSlot& slot, raft::ApplyCall& call)
{
    assert(slot.call_type == raft::CallType::Apply);
    
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

void raft_set_leader(bool val)
{
    raft::scoreboard->is_leader = val;
}

void* raft_shm_init()
{
    raft::init("raft", false);
    return raft::shm.get_address();
}

size_t raft_shm_size()
{
    return raft::shm.get_size();
}

void raft_start_client()
{
    pid_t kidpid = fork();
    if (kidpid == -1) {
        perror("Couldn't fork");
        abort();
    } else if (kidpid) {
        // parent, Go side
        fprintf(stderr, "Forked child: pid %d\n", kidpid);
        sleep(2);
    } else {
        // child, C side
        raft::run_client();
    }
}
