#include "common/circuit_breaker.h"
#include "common/logger.h"
#include <algorithm>

namespace taskflow {

CircuitBreaker::CircuitBreaker(const std::string& name, const Config& config)
    : name_(name), config_(config), state_(CircuitState::CLOSED),
      failure_count_(0), success_count_(0) {
}

bool CircuitBreaker::allowRequest() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    switch (state_.load()) {
        case CircuitState::CLOSED:
            return true;
            
        case CircuitState::OPEN: {
            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                now - last_failure_time_
            ).count();
            
            if (elapsed >= config_.recovery_timeout_seconds) {
                state_ = CircuitState::HALF_OPEN;
                LOG_INFO("Circuit breaker '{}' entering HALF_OPEN state", name_);
                return true;
            }
            return false;
        }
        
        case CircuitState::HALF_OPEN:
            return true;
            
        default:
            return false;
    }
}

void CircuitBreaker::recordSuccess() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (state_.load() == CircuitState::HALF_OPEN) {
        success_count_++;
        if (success_count_ >= config_.success_threshold) {
            resetToClosed();
        }
    } else if (state_.load() == CircuitState::CLOSED) {
        failure_count_ = 0;
    }
}

void CircuitBreaker::recordFailure() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    failure_count_++;
    last_failure_time_ = std::chrono::steady_clock::now();
    
    if (state_.load() == CircuitState::HALF_OPEN) {
        tripOpen();
    } else if (state_.load() == CircuitState::CLOSED) {
        if (failure_count_ >= config_.failure_threshold) {
            tripOpen();
        }
    }
}

CircuitState CircuitBreaker::getState() const {
    return state_.load();
}

std::string CircuitBreaker::getStateString() const {
    switch (state_.load()) {
        case CircuitState::CLOSED: return "CLOSED";
        case CircuitState::OPEN: return "OPEN";
        case CircuitState::HALF_OPEN: return "HALF_OPEN";
        default: return "UNKNOWN";
    }
}

void CircuitBreaker::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    resetToClosed();
}

void CircuitBreaker::tripOpen() {
    state_ = CircuitState::OPEN;
    success_count_ = 0;
    LOG_WARN("Circuit breaker '{}' tripped to OPEN state. Failures: {}", name_, failure_count_);
}

void CircuitBreaker::attemptReset() {
    if (success_count_ >= config_.success_threshold) {
        resetToClosed();
    }
}

void CircuitBreaker::resetToClosed() {
    state_ = CircuitState::CLOSED;
    failure_count_ = 0;
    success_count_ = 0;
    LOG_INFO("Circuit breaker '{}' reset to CLOSED state", name_);
}

CircuitBreakerManager& CircuitBreakerManager::instance() {
    static CircuitBreakerManager instance;
    return instance;
}

CircuitBreaker* CircuitBreakerManager::getBreaker(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = breakers_.find(name);
    if (it != breakers_.end()) {
        return it->second.get();
    }
    return nullptr;
}

CircuitBreaker* CircuitBreakerManager::getOrCreateBreaker(const std::string& name, const CircuitBreaker::Config& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = breakers_.find(name);
    if (it != breakers_.end()) {
        return it->second.get();
    }
    CircuitBreaker* ptr = new CircuitBreaker(name, config);
    breakers_[name] = std::unique_ptr<CircuitBreaker>(ptr);
    LOG_INFO("Created circuit breaker: {}", name);
    return ptr;
}

void CircuitBreakerManager::removeBreaker(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    breakers_.erase(name);
    LOG_INFO("Removed circuit breaker: {}", name);
}

void CircuitBreakerManager::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    breakers_.clear();
}

size_t CircuitBreakerManager::getBreakerCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return breakers_.size();
}

}
