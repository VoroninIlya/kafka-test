#pragma once

#include <string>

class MyProducer {
public:
    void producer_100ms(std::string server, std::string topic);
    void producer_1s(std::string server, std::string topic);
};
