#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

class PollingThreads {
public:
    void add_thread(std::thread polling_thread, std::function<void()> stop_function) {
        background_threads_.emplace_back(std::move(polling_thread));
        stop_functions_.emplace_back(std::move(stop_function));
    }

    PollingThreads() {
        std::cout << "PollingThreads::PollingThreads()" << std::endl;
    };

    ~PollingThreads() {
        std::cout << "PollingThreads::~PollingThreads()" << std::endl;

        for (auto& stop_function : stop_functions_) {
            if (stop_function) {
                stop_function();
            }
        }

        for (auto& t : background_threads_) {
            if (t.joinable()) {
                t.join();
            }
        }

        std::cout << "PollingThreads::~PollingThreads() done" << std::endl;
    }

private:
    PollingThreads& operator=(const PollingThreads&) = delete;
    PollingThreads(const PollingThreads&) = delete;
    PollingThreads& operator=(PollingThreads&&) = delete;
    PollingThreads(PollingThreads&&) = delete;

    std::vector<std::thread> background_threads_;
    std::vector<std::function<void()>> stop_functions_;
};

PollingThreads gPollingThreads;

int main() {
    auto flag = std::make_shared<std::atomic<bool>>(false);
    gPollingThreads.add_thread(
        std::thread([flag]() {
            while (!flag->load()) {
                std::cout << "Polling..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }),
        [flag]() { flag->store(true); }
    );
    std::this_thread::sleep_for(std::chrono::seconds(5));
    return 0;
}