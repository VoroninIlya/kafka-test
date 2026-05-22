#pragma once

#include <unordered_map>
#include <memory>
#include <future>

#include "ThreadPool.hpp"
#include "ProducerWorker.hpp"

namespace producer {

    struct ProducerHandle {
        std::unique_ptr<ProducerWorker> worker;
        std::future<void> future;
    };

    class ProducerManager {
    public:
        ProducerManager(ThreadPool& pool) : m_pool(pool) {}

        int start(const std::string& name, const std::string& brokers, 
            const std::string& topic, uint32_t periode_ms);

        void stop(int id);

        std::string list();

    private:
        ThreadPool& m_pool;
        std::unordered_map<int, ProducerHandle> m_producers{};
        std::mutex m_mtx{};
        int m_counter{0};
    };

};