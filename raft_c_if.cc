#include <cstdint>
#include <sys/types.h>
#include <thread>

#include "raft_c_if.h"
#include "raft_go_if.h"
#include "raft_shm.h"

using boost::interprocess::anonymous_instance;

void* raft_apply(char* cmd, size_t cmd_len, uint64_t timeout_ns)
{
    raft::SlotHandle sh(*raft::scoreboard);
    std::unique_lock<interprocess_mutex> l(sh.slot.owned);
    sh.slot.call_type = raft::CallType::Apply;
    sh.slot.state = raft::CallState::Pending;

    // manual memory management for now...
    raft::ApplyCall& call =
        *raft::shm.construct<raft::ApplyCall>(anonymous_instance)();
    fprintf(stderr, "Allocated call buffer at %p.\n", &call);

    call.cmd_buf = raft::shm.get_handle_from_address(cmd);
    //.offset = ((void*)cmd) - raft::shm.get_address();
    call.cmd_len = cmd_len;
    call.timeout_ns = timeout_ns;

    sh.slot.handle = raft::shm.get_handle_from_address(&call);
    sh.slot.call_ready = true;
    sh.slot.call_cond.notify_one();
    sh.slot.ret_cond.wait(l, [&] () { return sh.slot.ret_ready; });
    assert(sh.slot.state == raft::CallState::Success
           || sh.slot.state == raft::CallState::Error);

    sh.slot.ret_ready = false;

    return nullptr;
}

void* alloc_raft_buffer(size_t len)
{
    return raft::shm.allocate(len);
}

void free_raft_buffer(void* buf)
{
    raft::shm.deallocate(buf);
}
