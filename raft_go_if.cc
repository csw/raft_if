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

void dispatch_apply(api::Apply::slot_t& slot);
void dispatch_barrier(api::Barrier::slot_t& slot);
void dispatch_verify_leader(api::VerifyLeader::slot_t& slot);
void dispatch_snapshot(api::Snapshot::slot_t& slot);
void dispatch_add_peer(api::AddPeer::slot_t& slot);
void dispatch_remove_peer(api::RemovePeer::slot_t& slot);
void dispatch_shutdown(api::Shutdown::slot_t& slot);

const static uint32_t N_WORKERS = 4;

std::vector<std::thread> workers;

}

void raft_ready()
{
    workers.reserve(N_WORKERS);
    for (uint32_t i = 0; i < N_WORKERS; ++i) {
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
    case api::Apply::tag:
        dispatch_apply((api::Apply::slot_t&) *slot);
        break;
    case api::Barrier::tag:
        dispatch_barrier((api::Barrier::slot_t&) *slot);
        break;
    case api::VerifyLeader::tag:
        dispatch_verify_leader((api::VerifyLeader::slot_t&) *slot);
        break;
    case CallTag::Snapshot:
        dispatch_snapshot((api::Snapshot::slot_t&) *slot);
        break;
    case CallTag::AddPeer:
        dispatch_add_peer((api::AddPeer::slot_t&) *slot);
        break;
    case CallTag::RemovePeer:
        dispatch_remove_peer((api::RemovePeer::slot_t&) *slot);
        break;
    case CallTag::Shutdown:
        dispatch_shutdown((api::Shutdown::slot_t&) *slot);
        break;
    default:
        zlog_fatal(go_cat, "Unhandled call type: %d",
                   tag);
        abort();
    }
}

uintptr_t shm_offset(void* ptr)
{
    uintptr_t shm_base = (uintptr_t) shm.get_address();
    uintptr_t shm_end = (uintptr_t) shm_base + shm.get_size();
    uintptr_t address = (uintptr_t) ptr;
    assert(address >= shm_base && address < shm_end);
    return address - shm_base;
}

void dispatch_apply(api::Apply::slot_t& slot)
{
    assert(slot.tag == api::Apply::tag);
    
    size_t cmd_offset = shm_offset(slot.args.cmd_buf.get());

    slot.state = raft::CallState::Dispatched;
    RaftApply(&slot, cmd_offset, slot.args.cmd_len, slot.args.timeout_ns);
}

void dispatch_barrier(api::Barrier::slot_t& slot)
{
    slot.state = raft::CallState::Dispatched;
    RaftBarrier(&slot, slot.args.timeout_ns);
}

void dispatch_verify_leader(api::VerifyLeader::slot_t& slot)
{
    slot.state = raft::CallState::Dispatched;
    RaftVerifyLeader(&slot);
}

void dispatch_snapshot(api::Snapshot::slot_t& slot)
{
    assert(slot.tag == CallTag::Snapshot);
    slot.state = raft::CallState::Dispatched;
    RaftSnapshot(&slot);
}

void dispatch_add_peer(api::AddPeer::slot_t& slot)
{
    assert(slot.tag == CallTag::AddPeer);
    slot.state = raft::CallState::Dispatched;
    RaftAddPeer(&slot, (char*) slot.args.host, slot.args.port);
}

void dispatch_remove_peer(api::RemovePeer::slot_t& slot)
{
    assert(slot.tag == CallTag::RemovePeer);
    slot.state = raft::CallState::Dispatched;
    RaftRemovePeer(&slot, (char*) slot.args.host, slot.args.port);
}

void dispatch_shutdown(api::Shutdown::slot_t& slot)
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

namespace {

void raft_reply_(BaseSlot& slot, RaftError error)
{
    slot.timings.record("Raft call return");
    slot.reply(error);
}

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

    slot->timings.print();
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

RaftConfig* raft_get_config()
{
    return raft::shm.find<RaftConfig>(unique_instance).first;
}
