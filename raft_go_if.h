#ifndef RAFT_GO_IF_H
#define RAFT_GO_IF_H

#include "raft_defs.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include <sys/types.h>

// interface between Raft and the IPC layer, in C
// See also raft_if.go.

typedef void* raft_call;

void* raft_shm_init();
size_t raft_shm_size();

void raft_ready();

void raft_set_leader(bool val);

void raft_reply_apply(raft_call call, uint64_t retval, RaftError error);

uint64_t raft_fsm_apply(uint64_t index, uint64_t term, RaftLogType type,
                        char* cmd_buf, size_t cmd_len);

#ifdef __cplusplus
}
#endif

#endif /* RAFT_GO_IF_H */
