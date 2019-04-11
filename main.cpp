/*
Author: Joachim Joyaux

The aim of this program is to show how memory mapped
files can be used as support buffer for lock free
SPSC queue.

This code is intended to be use only on strong
memory model architectures (typically x86) because
it makes some assumptions on how atomic operations
are implemented.

The program is able to exchange more than 15'000'000
messages per seconds between two different processes
with Ubuntu 16.04 LTS / Intel Core i7-6500U @ 2.5GHz

The code should be compiled using
g++ -Wall -O3 -std=c++1{4,7} main.cpp -lrt
*/


#include <cassert>
#include <cstring>
#include <iostream>
#include <cstdint>
#include <chrono>
#include <thread>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#define SHM_SEGMENT_NAME "/shared_segment"
#define QUEUE_SIZE 4096
#define CACHE_LINE_SIZE 64
#define NB_MESSAGE_SENT 100'000'000
#define UNLIKELY(condition) __builtin_expect(static_cast<bool>(condition), 0)
#define LIKELY(condition) __builtin_expect(static_cast<bool>(condition), 1)

static_assert((QUEUE_SIZE & (QUEUE_SIZE - 1)) == 0, "QUEUE_SIZE must be a power of two");

struct Message
{
    char     buffer[128];
    uint64_t id;
};

struct SPSCQueue
{
    Message buffer[QUEUE_SIZE];

    // Avoid false sharing with padding
    uint64_t reader{0};
    char   _padding1[CACHE_LINE_SIZE - sizeof(reader)];

    uint64_t writer{0};
    char   _padding2[CACHE_LINE_SIZE - sizeof(writer)];

};

// Set the current process on maximum real time priority
void real_time_optimize(int cpu)
{
    const int max_fifo_priority = sched_get_priority_max(SCHED_FIFO);
    struct sched_param param;
    int pid = getpid();
    sched_getparam(pid, &param);
    param.sched_priority = max_fifo_priority;
    sched_setscheduler(pid, SCHED_FIFO, &param);
    cpu_set_t my_set;
    CPU_ZERO(&my_set);
    CPU_SET(cpu, &my_set);
    sched_setaffinity(0, sizeof(cpu_set_t), &my_set);
    mlockall(MCL_CURRENT | MCL_FUTURE);
}

void produce()
{
    real_time_optimize(0);
    std::cout << "Produce" << std::endl;
    int file_descriptor = shm_open(SHM_SEGMENT_NAME, O_RDWR | O_CREAT, 0600 );
    if( file_descriptor == -1 )
    {
        perror("Cannot open segment");
        return;
    }
    constexpr size_t size = sizeof(SPSCQueue);
    const int res = ftruncate(file_descriptor, size);
    if (res == -1)
    {
        perror("ftruncate()");
        return;
    }

    SPSCQueue* queue = reinterpret_cast<SPSCQueue*>(
        mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0)
    );
    if (reinterpret_cast<void*>(queue) == (void*)-1)
    {
        perror("mmap()");
        return;
    }
    close(file_descriptor);

    queue->writer = queue->reader = 0;

    uint64_t current_id = 0;
    for(;;)
    {
        // If the consumer is not lagging behind
        while(((queue->writer + 1) & (QUEUE_SIZE -1)) != ((queue->reader) & (QUEUE_SIZE - 1)))
        {
            Message* msg = &queue->buffer[queue->writer & (QUEUE_SIZE - 1)];
            msg->id = current_id++;
            __sync_fetch_and_add(&queue->writer, 1);
        }

        // pause for consumer which is lagging behind
        std::this_thread::yield();
        if (UNLIKELY(current_id > NB_MESSAGE_SENT))
        {
            break;
        }
    }
}

void consume()
{
    real_time_optimize(3);
    std::cout << "Consume" << std::endl;
    constexpr size_t size = sizeof(SPSCQueue);
    int file_descriptor = shm_open(SHM_SEGMENT_NAME, O_RDWR, 0600 );
    assert(file_descriptor != -1);

    SPSCQueue* queue = reinterpret_cast<SPSCQueue*>(
        mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, file_descriptor, 0)
    );

    uint64_t nb_message_read = 0;
    auto start = std::chrono::high_resolution_clock::now();
    for (;;)
    {
        // data to consume
        while (nb_message_read % QUEUE_SIZE != queue->writer % QUEUE_SIZE)
        {
            Message msg = queue->buffer[nb_message_read & (QUEUE_SIZE - 1)];
            assert(msg.id == nb_message_read);
            ++nb_message_read;
        }

        __sync_lock_test_and_set(&queue->reader, nb_message_read);

        if (UNLIKELY(nb_message_read > NB_MESSAGE_SENT))
        {
            std::cout << "DONE" << std::endl;
            break;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "Elapsed time: " << elapsed.count() << " s" << std::endl;
    std::cout << "Msg/s: " << NB_MESSAGE_SENT / elapsed.count() << " s" << std::endl;
    shm_unlink(SHM_SEGMENT_NAME);
}

int main()
{
    (fork() == 0) ? consume() : produce();
}
