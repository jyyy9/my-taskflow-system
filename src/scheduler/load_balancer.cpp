#include "scheduler/load_balancer.h"
#include "common/logger.h"
#include <algorithm>
#include <sstream>

namespace taskflow {

LoadBalancer::LoadBalancer(const std::string& strategy) 
    : strategy_(LoadBalanceStrategy::LEAST_CONNECTION), round_robin_index_(0) {
    setStrategy(strategy);
}

void LoadBalancer::setStrategy(const std::string& strategy) {
    if (strategy == "round_robin") {
        strategy_ = LoadBalanceStrategy::ROUND_ROBIN;
    } else if (strategy == "least_connection") {
        strategy_ = LoadBalanceStrategy::LEAST_CONNECTION;
    } else if (strategy == "consistent_hash") {
        strategy_ = LoadBalanceStrategy::CONSISTENT_HASH;
    } else {
        LOG_WARN("Unknown strategy '{}', defaulting to least_connection", strategy);
        strategy_ = LoadBalanceStrategy::LEAST_CONNECTION;
    }
    
    LOG_INFO("Load balancer strategy set to: {}", strategy);
}

void LoadBalancer::updateWorkers(const std::vector<ServiceInfo>& services) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::map<std::string, bool> current_workers;
    for (const auto& service : services) {
        current_workers[service.id] = true;
        
        auto it = workers_.find(service.id);
        if (it == workers_.end()) {
            WorkerNode node;
            node.id = service.id;
            node.address = service.address;
            node.port = service.port;
            node.current_load = 0;
            node.status = "active";
            node.last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count();
            node.last_check = std::chrono::steady_clock::now();
            
            workers_[service.id] = node;
            
            circuit_breakers_[service.id] = std::unique_ptr<CircuitBreaker>(
                new CircuitBreaker(service.id, CircuitBreaker::Config{3, 30, 1})
            );
            
            LOG_INFO("Worker added: {} at {}:{}", service.id, service.address, service.port);
        } else {
            it->second.address = service.address;
            it->second.port = service.port;
            it->second.last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count();
        }
    }
    
    for (auto it = workers_.begin(); it != workers_.end(); ) {
        if (current_workers.find(it->first) == current_workers.end()) {
            LOG_INFO("Worker removed: {}", it->first);
            circuit_breakers_.erase(it->first);
            it = workers_.erase(it);
        } else {
            ++it;
        }
    }
}

void LoadBalancer::updateWorkerLoad(const std::string& worker_id, int load) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = workers_.find(worker_id);
    if (it != workers_.end()) {
        it->second.current_load = load;
        LOG_DEBUG("Worker {} load updated to {}", worker_id, load);
    }
}

void LoadBalancer::removeWorker(const std::string& worker_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    workers_.erase(worker_id);
    circuit_breakers_.erase(worker_id);
    
    LOG_INFO("Worker removed from load balancer: {}", worker_id);
}

void LoadBalancer::markWorkerDead(const std::string& worker_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = workers_.find(worker_id);
    if (it != workers_.end()) {
        it->second.status = "dead";
        LOG_WARN("Worker marked as dead: {}", worker_id);
    }
}

std::string LoadBalancer::selectWorker(const std::string& task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (workers_.empty()) {
        LOG_WARN("No workers available for task selection");
        return "";
    }
    
    switch (strategy_) {
        case LoadBalanceStrategy::ROUND_ROBIN:
            return roundRobinSelect();
        case LoadBalanceStrategy::LEAST_CONNECTION:
            return leastConnectionSelect();
        case LoadBalanceStrategy::CONSISTENT_HASH:
            return consistentHashSelect(task_id);
        default:
            return leastConnectionSelect();
    }
}

std::string LoadBalancer::roundRobinSelect() {
    std::vector<std::string> available_workers;
    
    for (const auto& pair : workers_) {
        if (pair.second.status == "active") {
            auto cb_it = circuit_breakers_.find(pair.first);
            if (cb_it == circuit_breakers_.end() || 
                cb_it->second->allowRequest()) {
                available_workers.push_back(pair.first);
            }
        }
    }
    
    if (available_workers.empty()) {
        return "";
    }
    
    uint64_t index = round_robin_index_++ % available_workers.size();
    return available_workers[index];
}

std::string LoadBalancer::leastConnectionSelect() {
    WorkerNode* best = nullptr;
    std::string best_id;
    
    for (auto& pair : workers_) {
        if (pair.second.status != "active") {
            continue;
        }
        
        auto cb_it = circuit_breakers_.find(pair.first);
        if (cb_it != circuit_breakers_.end() && !cb_it->second->allowRequest()) {
            continue;
        }
        
        if (!best || pair.second.current_load < best->current_load) {
            best = &pair.second;
            best_id = pair.first;
        }
    }
    
    return best_id;
}

std::string LoadBalancer::consistentHashSelect(const std::string& task_id) {
    if (task_id.empty()) {
        return leastConnectionSelect();
    }
    
    uint64_t hash_value = hash(task_id);
    
    std::vector<std::pair<uint64_t, std::string>> circles;
    for (const auto& pair : workers_) {
        if (pair.second.status != "active") {
            continue;
        }
        
        auto cb_it = circuit_breakers_.find(pair.first);
        if (cb_it != circuit_breakers_.end() && !cb_it->second->allowRequest()) {
            continue;
        }
        
        circles.push_back({hash(pair.first), pair.first});
    }
    
    if (circles.empty()) {
        return "";
    }
    
    std::sort(circles.begin(), circles.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    
    for (const auto& circle : circles) {
        if (hash_value <= circle.first) {
            return circle.second;
        }
    }
    
    return circles[0].second;
}

uint64_t LoadBalancer::hash(const std::string& key) {
    uint64_t hash = 5381;
    for (char c : key) {
        hash = ((hash << 5) + hash) + static_cast<uint64_t>(c);
    }
    return hash;
}

std::vector<WorkerNode> LoadBalancer::getWorkers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<WorkerNode> result;
    for (const auto& pair : workers_) {
        result.push_back(pair.second);
    }
    return result;
}

WorkerNode* LoadBalancer::getWorker(const std::string& worker_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = workers_.find(worker_id);
    if (it != workers_.end()) {
        return &it->second;
    }
    return nullptr;
}

size_t LoadBalancer::getWorkerCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return workers_.size();
}

bool LoadBalancer::isWorkerAvailable(const std::string& worker_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = workers_.find(worker_id);
    if (it == workers_.end()) {
        return false;
    }
    
    if (it->second.status != "active") {
        return false;
    }
    
    auto cb_it = circuit_breakers_.find(worker_id);
    if (cb_it != circuit_breakers_.end() && !cb_it->second->allowRequest()) {
        return false;
    }
    
    return true;
}

void LoadBalancer::recordSuccess(const std::string& worker_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto cb_it = circuit_breakers_.find(worker_id);
    if (cb_it != circuit_breakers_.end()) {
        cb_it->second->recordSuccess();
    }
}

void LoadBalancer::recordFailure(const std::string& worker_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto cb_it = circuit_breakers_.find(worker_id);
    if (cb_it != circuit_breakers_.end()) {
        cb_it->second->recordFailure();
        
        if (cb_it->second->getState() == CircuitState::OPEN) {
            LOG_WARN("Circuit breaker opened for worker: {}", worker_id);
        }
    }
}

}
