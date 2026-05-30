#pragma once

#include <string>
#include <memory>
#include <queue>
#include <mutex>
#include <chrono>
#include "common/redis_client.h"
#include "scheduler/load_balancer.h"

namespace taskflow {

struct TaskRequest {
    std::string id;
    int type;
    int priority;
    std::string data;
    int64_t created_at;
    int retry_count;
    int max_retries;
    
    TaskRequest() : type(0), priority(1), created_at(0), retry_count(0), max_retries(3) {}
};

struct TaskResult {
    std::string task_id;
    bool success;
    std::string result;
    std::string error_message;
    int64_t completed_at;
};

class TaskDispatcher {
public:
    TaskDispatcher(std::shared_ptr<RedisPool> redis_pool,
                   std::shared_ptr<LoadBalancer> load_balancer);
    
    bool submitTask(const TaskRequest& task);
    std::string popNextTask();
    
    bool assignTaskToWorker(const std::string& task_id, const std::string& worker_id);
    bool completeTask(const std::string& task_id, const TaskResult& result);
    bool failTask(const std::string& task_id, const std::string& error);
    
    bool retryTask(const std::string& task_id);
    int getTaskRetryCount(const std::string& task_id);
    
    std::string getTaskStatus(const std::string& task_id);
    
    TaskInfo getTaskInfo(const std::string& task_id);
    
    size_t getQueueSize(int priority) const;
    size_t getTotalQueueSize() const;
    
private:
    std::shared_ptr<RedisPool> redis_pool_;
    std::shared_ptr<LoadBalancer> load_balancer_;
    
    std::mutex mutex_;
};

}
