#include <iostream>
#include <thread>
#include <string>
#include <sstream>
#include <fstream>
#include <csignal>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "ThreadPool.hpp"
#include "Registry.hpp"
#include "ConsumerWorker.hpp"

std::atomic<bool> globalStop(false);

static void cli(registry::RegistryManager& rm);

static void handleSigint(int) {
    globalStop = true;
}

int main(int argc, char* argv[]) {

    std::signal(SIGINT, handleSigint);

    threadpool::ThreadPool pool(4, 1000);
    registry::RegistryManager rm(pool);

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
            auto groupId = p["group_id"].GetString();

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

            p.Accept(writer);

            std::string jsonString = buffer.GetString();

            int id = rm.start(std::make_unique<consumer::ConsumerWorker>(jsonString));
            if (rm.isStarted(id)) {
                std::cout << "started id=" << id << " "
                          << "name: " << name << " "
                          << "brokers: " << brokers << " "
                          << "topic: " << topic << " "
                          << "group id: " << groupId << " "
                          << "\n";
            }
        }
    }

    std::thread cliThread([&] {
        cli(rm);
    });

    cliThread.join();

    pool.shutdown();

    return 0;
}

void cli(registry::RegistryManager& rm) {
    std::string cmd;

    while (true) {
        std::cout << "> ";
        std::cin >> cmd;

        if (cmd == "start") {
            std::string name, brokers, topic, groupId;

            std::cout << "name: ";
            std::cin >> name;

            std::cout << "brokers: ";
            std::cin >> brokers;

            std::cout << "topic: ";
            std::cin >> topic;

            std::cout << "group id: ";
            std::cin >> groupId;

            rapidjson::Document d;
            d.SetObject();
            auto& allocator = d.GetAllocator();
            rapidjson::Value obj(rapidjson::kObjectType);
            obj.AddMember("name", 
                rapidjson::Value(name.c_str(), allocator), allocator);
            obj.AddMember("brokers", 
                rapidjson::Value(brokers.c_str(), allocator), allocator);
            obj.AddMember("topic", 
                rapidjson::Value(topic.c_str(), allocator), allocator);
            obj.AddMember("group_id", 
                rapidjson::Value(groupId.c_str(), allocator), allocator);

            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

            obj.Accept(writer);

            int id = rm.start(std::make_unique<consumer::ConsumerWorker>(buffer.GetString()));
            if (rm.isStarted(id)) {
                std::cout << "started id=" << id << " "
                          << "name: " << name << " "
                          << "brokers: " << brokers << " "
                          << "topic: " << topic << " "
                          << "group id: " << groupId << " "
                          << "\n";
            }
        }

        else if (cmd == "stop") {
            int id;
            std::cout << "id: ";
            std::cin >> id;
            rm.stop(id);
        }

        else if (cmd == "list") {
            std::cout << rm.list();
        }

        else if (cmd == "exit") {
            break;
        }
    }
}