#include <librdkafka/rdkafkacpp.h>
#include <rapidjson/document.h>
#include <ctime>
#include <iomanip>
#include <chrono>
#include <thread>
#include <sstream>

#include "ConsumerWorker.hpp"
namespace consumer {

    ConsumerWorker::ConsumerWorker(const std::string json) {
        rapidjson::Document d;
        d.Parse(json.c_str());

        if (!d.IsObject()) return;

        if (!d.HasMember("name")) return;

        m_name = d["name"].GetString();

        if (d.HasMember("brokers")) 
            m_brokers = d["brokers"].GetString();

        if (d.HasMember("topic")) 
            m_topic = d["topic"].GetString();

        if (d.HasMember("group_id")) 
            m_groupId  = d["group_id"].GetString();

        m_isStarted = true;
    }

    void ConsumerWorker::run() {
        std::string errstr;

        // -------- CONFIG --------
        RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

        conf->set("bootstrap.servers", m_brokers, errstr);
        conf->set("group.id", m_groupId, errstr);
        conf->set("auto.offset.reset", "earliest", errstr);

        // -------- CREATE CONSUMER --------
        RdKafka::KafkaConsumer *consumer =
            RdKafka::KafkaConsumer::create(conf, errstr);

        if (!consumer) {
            //std::cerr << "Failed to create consumer: " << errstr << std::endl;
            return;
        }

        delete conf;

        // -------- SUBSCRIBE --------
        std::vector<std::string> topics = {m_topic};

        RdKafka::ErrorCode err = consumer->subscribe(topics);
        if (err) {
            // std::cerr << "Subscribe failed: "
            //           << RdKafka::err2str(err) << std::endl;
            return;
        }

        //std::cout << "Consumer started...\n";

        while (!m_stop.load()) {
            RdKafka::Message *msg = consumer->consume(1000);

            switch (msg->err()) {
                case RdKafka::ERR_NO_ERROR:
                    //std::cout << "Message: "
                    //          << std::string((char*)msg->payload(),
                    //                         msg->len())
                    //          << std::endl;
                    break;

                case RdKafka::ERR__TIMED_OUT:
                    // no message, normal
                    break;

                default:
                    //std::cerr << "Error: "
                    //          << msg->errstr() << std::endl;
                    break;
            }

            delete msg;
        }

        consumer->close();
        delete consumer;
    }

    void ConsumerWorker::stop() {
        m_stop.store(true);
    }

    bool ConsumerWorker::isStarted() const {
        return m_isStarted;
    }

    void ConsumerWorker::print(std::ostream& os) const {
        os  << "name: " << m_name << " "
            << "brokers: " << m_brokers << " "
            << "topic: " << m_topic << " "
            << "group_id: " << m_groupId << " ";
    }

    std::string ConsumerWorker::getName() const {
        return m_name;
    }

    std::string ConsumerWorker::getBrokers() const {
        return m_brokers;
    }

    std::string ConsumerWorker::getTopic() const {
        return m_topic;
    }

    std::string ConsumerWorker::getGroupId() const {
        return m_groupId;
    }

}