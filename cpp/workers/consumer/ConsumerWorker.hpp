#pragma once

#include <atomic>
#include <string>

#include "IWorker.hpp"

namespace consumer {

    class ConsumerWorker : public workers::IWorker {
    public:
        ConsumerWorker(const std::string json);

        virtual void run() override;
        virtual void stop() override;
        virtual bool isStarted() const override;

        virtual void print(std::ostream& os) const override;

        std::string getName() const;
        std::string getBrokers() const;
        std::string getTopic() const;
        std::string getGroupId() const;

    private:
        std::string m_name;
        std::string m_brokers;
        std::string m_topic;
        std::string m_groupId;
        std::atomic<bool> m_stop;
        bool m_isStarted{};

    };

};