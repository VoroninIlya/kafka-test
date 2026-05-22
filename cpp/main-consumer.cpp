#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <memory>

int main() {
    std::string errstr;

    // -------- CONFIG --------
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    conf->set("bootstrap.servers", "192.168.1.105:9094", errstr);
    conf->set("group.id", "test-consumer-group", errstr);
    conf->set("auto.offset.reset", "earliest", errstr);

    // -------- CREATE CONSUMER --------
    RdKafka::KafkaConsumer *consumer =
        RdKafka::KafkaConsumer::create(conf, errstr);

    if (!consumer) {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        return 1;
    }

    delete conf;

    // -------- SUBSCRIBE --------
    std::vector<std::string> topics = {"test-topic-1"};

    RdKafka::ErrorCode err = consumer->subscribe(topics);
    if (err) {
        std::cerr << "Subscribe failed: "
                  << RdKafka::err2str(err) << std::endl;
        return 1;
    }

    std::cout << "Consumer started...\n";

    // -------- POLL LOOP --------
    while (true) {
        RdKafka::Message *msg = consumer->consume(1000);

        switch (msg->err()) {
            case RdKafka::ERR_NO_ERROR:
                std::cout << "Message: "
                          << std::string((char*)msg->payload(),
                                         msg->len())
                          << std::endl;
                break;

            case RdKafka::ERR__TIMED_OUT:
                // no message, normal
                break;

            default:
                std::cerr << "Error: "
                          << msg->errstr() << std::endl;
                break;
        }

        delete msg;
    }

    consumer->close();
    delete consumer;

    return 0;
}