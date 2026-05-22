#include <librdkafka/rdkafkacpp.h>
#include <ctime>
#include <iomanip>
#include <chrono>
#include <thread>
#include <sstream>

#include "ProducerWorker.hpp"

namespace producer {

    class DeliveryReportCb : public RdKafka::DeliveryReportCb {
    public:
        void dr_cb(RdKafka::Message &message) override {
            std::ostringstream oss;
            if (message.err()) {
                oss << "Delivery failed: "
                          << message.errstr() << std::endl;
            } else {
                oss << "Delivered to topic "
                    << message.topic_name()
                    << " [" << message.partition()
                    << "] offset "
                    << message.offset()
                    << std::endl;
            }
        }
    };

    void ProducerWorker::run() {
        std::string errstr;
        // -------- CONFIG --------
        RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

        conf->set("bootstrap.servers", m_brokers.c_str(), errstr);

        DeliveryReportCb dr_cb;
        conf->set("dr_cb", &dr_cb, errstr);

        // -------- CREATE PRODUCER --------
        RdKafka::Producer *producer =
            RdKafka::Producer::create(conf, errstr);

        if (!producer) {
            //std::cerr << "Failed to create producer: " << errstr << std::endl;
            return;
        }

        delete conf;

        auto next = std::chrono::steady_clock::now();

        uint64_t index = 0;

        while (!m_stop.load()) {
            
            next += std::chrono::milliseconds(m_period_ms);

            auto now = std::chrono::system_clock::now();
            std::time_t now_c = std::chrono::system_clock::to_time_t(now);
            std::tm* tm = std::localtime(&now_c);
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

            std::ostringstream oss;
            oss << "{";
            oss << "\"producer\": \"" << m_name << "\",";
            oss << "\"time\": \"" << std::put_time(tm,"%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0') << std::setw(3) << ms.count() <<"\",";
            oss << "\"message\": \"counter: " << index++ << "\"";
            oss << "}";

            std::string msg = oss.str();

            RdKafka::ErrorCode resp = producer->produce(
                 /* Topic name */
                m_topic,
                /* Any Partition: the builtin partitioner will be
                 * used to assign the message to a topic based
                 * on the message key, or random partition if
                 * the key is not set. */
                RdKafka::Topic::PARTITION_UA,
                /* Make a copy of the value */
                RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                /* Value */
                const_cast<char*>(msg.c_str()), msg.size(),
                /* Key */
                nullptr, 0,
                /* Timestamp (defaults to current time) */
                0,
                /* Message headers, if any */
                nullptr,
                /* Per-message opaque value passed to
                 * delivery report */
                nullptr
            );

            if (resp != RdKafka::ERR_NO_ERROR) {
                //std::cerr << "Produce failed: "
                //          << RdKafka::err2str(resp) << std::endl;
                return;
            }

            producer->poll(0); // trigger delivery callbacks

            std::this_thread::sleep_until(next);
        }
    }

    void ProducerWorker::stopProducer() {
        m_stop.store(true);
    }

    std::string ProducerWorker::getName() {
        return m_name;
    }

    std::string ProducerWorker::getBrokers() {
        return m_brokers;
    }

    std::string ProducerWorker::getTopic() {
        return m_topic;
    }

    uint32_t ProducerWorker::getPeriodMs() {
        return m_period_ms;
    }

}