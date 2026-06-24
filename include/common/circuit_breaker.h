#pragma once

#include <string>
#include <mutex>
#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>

namespace taskflow {

enum class CircuitState {
    CLOSED,
    OPEN,
    HALF_OPEN
};

class CircuitBreaker {
public:
    struct Config {
        int failure_threshold;
        int recovery_timeout_seconds;
        int success_threshold;
        
        Config() : failure_threshold(3), recovery_timeout_seconds(30), success_threshold(1) {}
    };
    
    CircuitBreaker(const std::string& name, const Config& config);
    
    bool allowRequest();
    void recordSuccess();
    void recordFailure();
    
    CircuitState getState() const;
    std::string getStateString() const;
    
    int getFailureCount() const { return failure_count_; }
    int getSuccessCount() const { return success_count_; }
    
    void reset();

private:
    void tripOpen();
    void attemptReset();
    void resetToClosed();
    
    std::string name_;
    Config config_;
    
    std::atomic<CircuitState> state_;
    std::atomic<int> failure_count_;
    std::atomic<int> success_count_;
    std::chrono::steady_clock::time_point last_failure_time_;
    std::mutex mutex_;
};

class CircuitBreakerManager {
public:
    static CircuitBreakerManager& instance();
    
    CircuitBreaker* getBreaker(const std::string& name);
    CircuitBreaker* getOrCreateBreaker(const std::string& name, const CircuitBreaker::Config& config);
    
    void removeBreaker(const std::string& name);
    void clear();
    
    size_t getBreakerCount() const;

private:
    CircuitBreakerManager() = default;
    ~CircuitBreakerManager() = default;
    CircuitBreakerManager(const CircuitBreakerManager&) = delete;
    CircuitBreakerManager& operator=(const CircuitBreakerManager&) = delete;
    
    std::map<std::string, std::unique_ptr<CircuitBreaker>> breakers_;
    mutable std::mutex mutex_;
};

}
