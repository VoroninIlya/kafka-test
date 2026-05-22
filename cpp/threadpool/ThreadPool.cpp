
#include <mutex>
#include <future>

#include "ThreadPool.hpp"

namespace threadpool {

    ThreadPool::ThreadPool(size_t threads, size_t maxQueue)
        : m_stop(false), m_maxQueueSize(maxQueue) {
        for (size_t i = 0; i < threads; ++i) {
            m_workers.emplace_back([this] { workerLoop(); });
        }
    }
    
    ThreadPool::~ThreadPool() {
        shutdown();
    }
    
    void ThreadPool::workerLoop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(m_mtx);
                m_cv.wait(lock, [this] {
                    return m_stop || !m_tasks.empty();
                });
                if (m_stop && m_tasks.empty())
                    return;
                task = std::move(m_tasks.front());
                m_tasks.pop();
                m_cvFull.notify_one();
            }
            task();
        }
    }
    
    void ThreadPool::shutdown() {
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            m_stop = true;
        }
        m_cv.notify_all();
        for (auto& t : m_workers)
            if (t.joinable())
                t.join();
    }

}