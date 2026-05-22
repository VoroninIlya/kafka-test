#include <librdkafka/rdkafkacpp.h>
#include <rapidjson/document.h>
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

    ProducerWorker::ProducerWorker(const std::string json) : m_stop(false) {
        rapidjson::Document d;
        d.Parse(json.c_str());

        if (!d.IsObject()) return;

        if (!d.HasMember("name")) return;

        m_name = d["name"].GetString();

        if (d.HasMember("brokers")) 
            m_brokers = d["brokers"].GetString();

        if (d.HasMember("topic")) 
            m_topic = d["topic"].GetString();

        if (d.HasMember("period_ms")) 
            m_period_ms  = d["period_ms"].GetInt();

        m_isStarted = true;
    }

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
        m_isStarted = false;
    }

    void ProducerWorker::print(std::ostream& os) const {
        os  << "name: " << m_name << " "
            << "brokers: " << m_brokers << " "
            << "topic: " << m_topic << " "
            << "period[ms]: " << m_period_ms << " ";
    }

    bool ProducerWorker::isStarted() const {
        return m_isStarted;
    }

    void ProducerWorker::stop() {
        m_stop.store(true);
    }

    std::string ProducerWorker::getName() const {
        return m_name;
    }

    std::string ProducerWorker::getBrokers() const {
        return m_brokers;
    }

    std::string ProducerWorker::getTopic() const {
        return m_topic;
    }

    uint32_t ProducerWorker::getPeriodMs() const {
        return m_period_ms;
    }

}