
#include <iostream>
#include <thread>
#include <string>
#include <sstream>
#include <fstream>
#include <csignal>

#include <rapidjson/document.h>

//#include "producer/producer.hpp"
#include "producer/ThreadPool.hpp"
#include "producer/ProducerRegistry.hpp"

std::atomic<bool> globalStop(false);

static void cli(producer::ProducerManager& pm);

static void handleSigint(int) {
    globalStop = true;
}

int main(int argc, char* argv[]) {

    std::signal(SIGINT, handleSigint);

    producer::ThreadPool pool(4, 1000);
    producer::ProducerManager pm(pool);

    if (argc == 2) {
        std::string configPath = argv[1];

        std::ifstream file(configPath);

        if (!file.is_open()) {
            std::cerr << "Cannot open file\n";
            return 1;
        }

        std::stringstream buffer;
        buffer << file.rdbuf();

        std::string json = buffer.str();

        rapidjson::Document d;
        d.Parse(json.c_str());

        if (!d.IsObject()) {
            std::cerr << "Invalid JSON\n";
            return 1;
        }

        const auto& producers = d["producers"];

        for (auto& p : producers.GetArray()) {

            auto name = p["name"].GetString();
            auto brokers = p["brokers"].GetString();
            auto topic = p["topic"].GetString();
            auto period_ms = p["period_ms"].GetInt();

            int id = pm.start(name, brokers, topic, period_ms);
            std::cout << "started id=" << id 
                      << "name: " << name << " "
                      << "brokers: " << brokers << " "
                      << "topic: " << topic << " "
                      << "period[ms]: " << period_ms << " "
                      << "\n";
        }
    }

    std::thread cliThread([&] {
        cli(pm);
    });

    cliThread.join();

    pool.shutdown();

    return 0;

    //MyProducer prc;
//
    //std::thread t1(&MyProducer::producer_100ms, &prc, "192.168.1.103:9094", "test-topic-1");
    //std::thread t2(&MyProducer::producer_1s, &prc, "192.168.1.104:9094", "test-topic-2");
//
    //t1.join();
    //t2.join();
//
    //return 0;
}

void cli(producer::ProducerManager& pm) {
    std::string cmd;

    while (true) {
        std::cout << "> ";
        std::cin >> cmd;

        if (cmd == "start") {
            std::string name, brokers, topic;
            uint32_t period_ms;

            std::cout << "name: ";
            std::cin >> name;

            std::cout << "brokers: ";
            std::cin >> brokers;

            std::cout << "topic: ";
            std::cin >> topic;

            std::cout << "period[ms]: ";
            std::cin >> period_ms;

            int id = pm.start(name, brokers, topic, period_ms);
            std::cout << "started id=" << id 
                      << "name: " << name << " "
                      << "brokers: " << brokers << " "
                      << "topic: " << topic << " "
                      << "period[ms]: " << period_ms << " "
                      << "\n";
        }

        else if (cmd == "stop") {
            int id;
            std::cout << "id: ";
            std::cin >> id;
            pm.stop(id);
        }

        else if (cmd == "list") {
            std::cout << pm.list();
        }

        else if (cmd == "exit") {
            break;
        }
    }
}