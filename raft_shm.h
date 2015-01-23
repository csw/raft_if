// -*- c++ -*-
#ifndef RAFT_SHM_H
#define RAFT_SHM_H

#include <vector>

#include <boost/interprocess/managed_shared_memory.hpp>

#include "channel.h"

namespace raft {

using boost::interprocess::managed_shared_memory;

const static size_t SHM_SIZE = 64 * 1024 * 1024;

/*
struct Scoreboard {
    Channel<APICall> api_channels[16];
};
*/

extern managed_shared_memory shm;

void init(const char* name, bool create);

}

#endif /* RAFT_SHM_H */
