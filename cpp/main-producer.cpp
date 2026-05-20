#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <string>

class DeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message &message) override {
        if (message.err()) {
            std::cerr << "Delivery failed: "
                      << message.errstr() << std::endl;
        } else {
            std::cout << "Delivered to topic "
                      << message.topic_name()
                      << " [" << message.partition()
                      << "] offset "
                      << message.offset()
                      << std::endl;
        }
    }
};

int main() {
    std::string errstr;

    // -------- CONFIG --------
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    conf->set("bootstrap.servers", "192.168.1.103:9094", errstr);

    DeliveryReportCb dr_cb;
    conf->set("dr_cb", &dr_cb, errstr);

    // -------- CREATE PRODUCER --------
    RdKafka::Producer *producer =
        RdKafka::Producer::create(conf, errstr);

    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return 1;
    }

    delete conf;

    std::string topic = "test-topic";

    // -------- SEND MESSAGES --------
    for (int i = 0; i < 10; i++) {
        std::string msg = "message " + std::to_string(i);

        RdKafka::ErrorCode resp = producer->produce(
             /* Topic name */
            topic,
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
            std::cerr << "Produce failed: "
                      << RdKafka::err2str(resp) << std::endl;
        }

        producer->poll(0); // trigger delivery callbacks
    }

    // -------- FLUSH --------
    std::cout << "Flushing...\n";
    producer->flush(5000);

    delete producer;

    return 0;
}