#include <mutex>
#include <iostream>
#include <sstream>

#include "Registry.hpp" 

namespace registry {

    int RegistryManager::start(std::unique_ptr<workers::IWorker> worker) {
        std::lock_guard<std::mutex> lock(m_mtx);

        int id = ++m_counter;

        auto future = m_pool.enqueue([w = worker.get()] {
            w->run();
        });

        m_producers[id] = {
            std::move(worker),
            std::move(future)
        };

        return id;
    }

    void RegistryManager::stop(int id) {
        std::lock_guard<std::mutex> lock(m_mtx);

        auto it = m_producers.find(id);
        if (it != m_producers.end()) {
            it->second.worker->stop();
            m_producers.erase(it);
        }
    }

    bool RegistryManager::isStarted(int id) {
        std::lock_guard<std::mutex> lock(m_mtx);

        auto it = m_producers.find(id);
        if (it != m_producers.end()) {
            return it->second.worker->isStarted();
        }
        return false;
    }

    std::string RegistryManager::list() {
        std::lock_guard<std::mutex> lock(m_mtx);
        std::ostringstream oss;

        for (auto& [id, p] : m_producers) {
            oss << "id: " << id << " "
                << *p.worker
                << "\n";
        }

        return oss.str();
    }

}