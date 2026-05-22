#pragma once

#include <atomic>
#include <string>

#include "IWorker.hpp"

namespace producer {

    class ProducerWorker : public workers::IWorker {
    public:
        ProducerWorker(const std::string json);

        virtual void run() override;
        virtual void stop() override;
        virtual bool isStarted() const override;

        virtual void print(std::ostream& os) const override;

        std::string getName() const;
        std::string getBrokers() const;
        std::string getTopic() const;
        uint32_t getPeriodMs() const;

    private:
        std::string m_name;
        std::string m_brokers;
        std::string m_topic;
        std::atomic<bool> m_stop;
        bool m_isStarted{};

        uint32_t m_period_ms{};
    };

};
