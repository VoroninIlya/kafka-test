#include <mutex>
#include <iostream>
#include <sstream>

#include "ProducerRegistry.hpp" 

namespace producer {

    int ProducerManager::start(const std::string& name, const std::string& brokers, 
            const std::string& topic, uint32_t periode_ms) {
        std::lock_guard<std::mutex> lock(m_mtx);

        int id = ++m_counter;

        auto worker = std::make_unique<ProducerWorker>(name, brokers, topic, periode_ms);

        auto future = m_pool.enqueue([w = worker.get()] {
            w->run();
        });

        m_producers[id] = {
            std::move(worker),
            std::move(future)
        };

        return id;
    }

    void ProducerManager::stop(int id) {
        std::lock_guard<std::mutex> lock(m_mtx);

        auto it = m_producers.find(id);
        if (it != m_producers.end()) {
            it->second.worker->stopProducer();
            m_producers.erase(it);
        }
    }

    std::string ProducerManager::list() {
        std::lock_guard<std::mutex> lock(m_mtx);
        std::ostringstream oss;

        for (auto& [id, p] : m_producers) {
            oss << "id: " << id << " "
                << "name: " << p.worker->getName() << " "
                << "brokers: " << p.worker->getBrokers() << " "
                << "topic: " << p.worker->getTopic() << " "
                << "period[ms]: " << p.worker->getPeriodMs() << " "
                << "\n";
        }

        return oss.str();
    }

}