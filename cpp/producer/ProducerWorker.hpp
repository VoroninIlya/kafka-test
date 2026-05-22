#pragma once

#include <atomic>
#include <string>

namespace producer {

    class ProducerWorker {
    public:
        ProducerWorker(const std::string name, const std::string& brokers, 
            const std::string& topic, uint32_t period_ms)
            : m_name(name), m_brokers(brokers), m_topic(topic), m_period_ms(period_ms), m_stop(false) {}

        void run();
        void stopProducer();

        std::string getName();
        std::string getBrokers();
        std::string getTopic();
        uint32_t getPeriodMs();

    private:
        std::string m_name;
        std::string m_brokers;
        std::string m_topic;
        std::atomic<bool> m_stop;

        uint32_t m_period_ms{};
    };

};
