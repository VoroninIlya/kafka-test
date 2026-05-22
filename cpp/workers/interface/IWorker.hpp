#pragma once

namespace workers {

    struct IWorker {
        virtual void run() = 0;
        virtual void stop() = 0;

        virtual bool isStarted() const = 0;
        virtual void print(std::ostream& os) const = 0;

        friend std::ostream& operator<<(
            std::ostream& os,
            const IWorker& obj) {
            obj.print(os);
            return os;
        }

        virtual ~IWorker() = default;
    };

    

};