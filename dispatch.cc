#include <cassert>
#include <cstdlib>
#include <thread>

#include "zlog/src/zlog.h"

#include "raft_go_if.h"
#include "raft_shm.h"
#include "queue.h"

#include "dispatch.h"

extern "C" {
#include "_cgo_export.h"
}

using namespace raft;

namespace dispatch {

namespace {
void run_worker();
void take_call();
uintptr_t shm_offset(void* ptr);

#define api_call(name, arg_t, has_ret)                  \
    void dispatch_ ## name(api::name::slot_t& slot);
#include "raft_api_calls.h"
#undef api_call

} // end anon namespace

std::thread start_worker()
{
    return std::thread(run_worker);
}

namespace {

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
    zlog_debug(go_cat, "API call received, tag %s, call %p.",
               tag_name(tag), slot.get());
    assert(slot->state == raft::CallState::Pending);

    switch (tag) {
#define api_call(name, arg_t, has_ret)                  \
        case api::name::tag: \
            dispatch_ ## name((api::name::slot_t&) *slot); \
            break;
#include "raft_api_calls.h"
#undef api_call
    default:
        zlog_fatal(go_cat, "Unhandled call type: %s",
                   tag_name(tag));
        abort();
    }
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

uintptr_t shm_offset(void* ptr)
{
    uintptr_t shm_base = (uintptr_t) shm.get_address();
    uintptr_t shm_end = (uintptr_t) shm_base + shm.get_size();
    uintptr_t address = (uintptr_t) ptr;
    assert(address >= shm_base && address < shm_end);
    return address - shm_base;
}

} // end anon namespace

} // end namespace dispatch
