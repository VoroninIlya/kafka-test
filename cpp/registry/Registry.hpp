#pragma once

#include <unordered_map>
#include <memory>
#include <future>

#include "ThreadPool.hpp"
#include "IWorker.hpp"

namespace registry {

    struct RegistryHandle {
        std::unique_ptr<workers::IWorker> worker;
        std::future<void> future;
    };

    class RegistryManager {
    public:
        RegistryManager(threadpool::ThreadPool& pool) : m_pool(pool) {}

        int start(std::unique_ptr<workers::IWorker> worker);

        void stop(int id);
        bool isStarted(int id);

        std::string list();

    private:
        threadpool::ThreadPool& m_pool;
        std::unordered_map<int, RegistryHandle> m_producers{};
        std::mutex m_mtx{};
        int m_counter{0};
    };

};