#pragma once

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <functional>
#include <mutex>
#include <atomic>
#include <chrono>
#include "common/etcd_client.h"
#include "common/circuit_breaker.h"

namespace taskflow {

struct WorkerNode {
    std::string id;
    std::string address;
    int port;
    int current_load;
    std::string status;
    int64_t last_heartbeat;
    std::chrono::steady_clock::time_point last_check;
};

enum class LoadBalanceStrategy {
    ROUND_ROBIN,
    LEAST_CONNECTION,
    CONSISTENT_HASH
};

class LoadBalancer {
public:
    LoadBalancer(const std::string& strategy = "least_connection");
    
    void setStrategy(const std::string& strategy);
    LoadBalanceStrategy getStrategy() const { return strategy_; }
    
    void updateWorkers(const std::vector<ServiceInfo>& services);
    void updateWorkerLoad(const std::string& worker_id, int load);
    void removeWorker(const std::string& worker_id);
    void markWorkerDead(const std::string& worker_id);
    
    std::string selectWorker(const std::string& task_id = "");
    std::vector<WorkerNode> getWorkers() const;
    WorkerNode* getWorker(const std::string& worker_id);
    size_t getWorkerCount() const;
    
    bool isWorkerAvailable(const std::string& worker_id);
    
    void recordSuccess(const std::string& worker_id);
    void recordFailure(const std::string& worker_id);
    
private:
    std::string roundRobinSelect();
    std::string leastConnectionSelect();
    std::string consistentHashSelect(const std::string& task_id);
    
    uint64_t hash(const std::string& key);
    
    LoadBalanceStrategy strategy_;
    std::map<std::string, WorkerNode> workers_;
    std::atomic<uint64_t> round_robin_index_;
    mutable std::mutex mutex_;
    
    std::map<std::string, std::unique_ptr<CircuitBreaker>> circuit_breakers_;
};

}
