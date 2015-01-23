#ifndef RAFT_GO_IF_H
#define RAFT_GO_IF_H

#ifdef __cplusplus
extern "C" {
#endif

    #include <sys/types.h>

    // interface between Raft and the IPC layer, in C

    void* raft_shm_init();
    size_t raft_shm_size();

    // goroutines call this to wait for a command
    void await_call();

    /*
     * client allocates buffer, writes command
     * client calls Apply()
     * passed via shm to waiting goroutine
     * await_call() invokes raft_apply()
     */

    //apply_future_t raft_apply(char *cmd, uint64_t timeout_ns);
    


#ifdef __cplusplus
}
#endif

#endif /* RAFT_GO_IF_H */
