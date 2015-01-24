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

    void* raft_shm_init();
    size_t raft_shm_size();

    void raft_ready();

    void raft_set_leader(bool val);

    typedef struct {
        size_t offset;
        size_t len;
    } raft_buffer_t;

    typedef struct {
        // call
        raft_buffer_t   cmd_buf;
        uint64_t        timeout_ns;
        // response
        // policy?
        uint64_t        dispatch_ns;
        raft_buffer_t   response_buf;

        uint32_t        errlen;
        char            errmsg[64];
    } raft_apply_call_t;

    /*
     * client allocates buffer, writes command
     * client calls Apply()
     * passed via shm to waiting goroutine
     * await_call() invokes raft_apply()
     * Raft creates a future, returns it
     *
     * Raft eventually invokes FSM::Apply()
     * FSM returns something (on the C side) [in an allocated reply buffer]
     * Raft populates the future appropriately
     * a waiting goroutine should place a pointer to the reply in the future
     *   if the future is unreferenced on the client side, free it
     */

    //apply_future_t raft_apply(char *cmd, uint64_t timeout_ns);

    uint64_t raft_fsm_apply(uint64_t index, uint64_t term, RaftLogType type,
                            char* cmd_buf, size_t cmd_len);

#ifdef __cplusplus
}
#endif

#endif /* RAFT_GO_IF_H */
