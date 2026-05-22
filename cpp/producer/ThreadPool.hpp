#pragma once

#include <vector>
#include <future>
#include <thread>
#include <queue>
#include <condition_variable>
#include <functional>
#include <atomic>

namespace producer {

    class ThreadPool {
    public:
        ThreadPool(size_t threads, size_t maxQueue = 1000);

        ~ThreadPool();

        void workerLoop();

        void shutdown();

        template<class F>
        auto enqueue(F&& f) -> std::future<decltype(f())> {
            using R = decltype(f());

            auto task = std::make_shared<std::packaged_task<R()>>(std::forward<F>(f));

            std::future<R> res = task->get_future();

            {
                std::unique_lock<std::mutex> lock(m_mtx);

                m_cvFull.wait(lock, [this] {
                    return m_tasks.size() < m_maxQueueSize || m_stop;
                });

                if (m_stop)
                    throw std::runtime_error("ThreadPool stopped");

                m_tasks.emplace([task]() { (*task)(); });
            }

            m_cv.notify_one();
            return res;
        }

    private:
        size_t m_maxQueueSize{};
        bool m_stop{};

        std::vector<std::thread> m_workers;
        std::queue<std::function<void()>> m_tasks;

        std::mutex m_mtx;
        std::condition_variable m_cv;
        std::condition_variable m_cvFull;
    };

};