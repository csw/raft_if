// -*- c++ -*-

#ifndef CHANNEL_H
#define CHANNEL_H

#include <cassert>
#include <mutex>

#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>

using boost::interprocess::interprocess_condition;
using boost::interprocess::interprocess_mutex;

template<typename T> class Channel
{
public:
    void put(T&& val);
    T take();
    
private:
    interprocess_mutex m;
    interprocess_condition cond_put_ready;
    interprocess_condition cond_take_ready;
    bool put_ready;
    bool take_ready;
    T xfer_val;

    void reset();
};

template<typename T>
void Channel<T>::put(T&& val)
{
    std::unique_lock<interprocess_mutex> l(m);
    assert(! put_ready);
    put_ready = true;
    xfer_val = std::move(val);

    while (! take_ready) {
        cond_take_ready.wait(l);
    }

    cond_put_ready.notify_one();

    reset();
}

template<typename T>
T Channel<T>::take()
{
    std::unique_lock<interprocess_mutex> l(m);
    assert(! take_ready);
    take_ready = true;

    while (! put_ready) {
        cond_put_ready.wait(l);
    }

    cond_take_ready.notify_one();
    return std::move(xfer_val);
}

template<typename T>
void Channel<T>::reset()
{
    put_ready = false;
    take_ready = false;
}

#endif /* CHANNEL_H */
