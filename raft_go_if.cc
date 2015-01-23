#include "raft_go_if.h"
#include "raft_shm.h"

void* raft_shm_init()
{
    raft::init("raft", true);
    return raft::shm.get_address();
}

size_t raft_shm_size()
{
    return raft::shm.get_size();
}
