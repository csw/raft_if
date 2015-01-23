#include "raft_shm.h"

namespace raft {

managed_shared_memory shm;

void init(const char* name, bool create)
{
    // [create]
    // register on-exit callback to call remove()
    if (create) {
        shm = managed_shared_memory(boost::interprocess::open_or_create_t{}, 
                                    "raft", SHM_SIZE);
    } else {
        shm = managed_shared_memory(boost::interprocess::open_only_t{},
                                    "raft");
    }
}

}
