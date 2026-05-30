#include "scheduler/task_dispatcher.h"
#include "common/logger.h"
#include <chrono>

namespace taskflow {

TaskDispatcher::TaskDispatcher(std::shared_ptr<RedisPool> redis_pool,
                               std::shared_ptr<LoadBalancer> load_balancer)
    : redis_pool_(redis_pool), load_balancer_(load_balancer) {
}

bool TaskDispatcher::submitTask(const TaskRequest& task) {
    auto redis = redis_pool_->getConnection();
    if (!redis) {
        LOG_ERROR("Failed to get Redis connection for task submission");
        return false;
    }
    
    TaskInfo task_info;
    task_info.id = task.id;
    task_info.type = task.type;
    task_info.priority = task.priority;
    task_info.data = task.data;
    task_info.created_at = task.created_at;
    task_info.status = 0;
    task_info.retry_count = 0;
    
    if (!redis->saveTask(task_info)) {
        LOG_ERROR("Failed to save task {}", task.id);
        redis_pool_->returnConnection(redis);
        return false;
    }
    
    if (!redis->pushToPriorityQueue(task.priority, task.id)) {
        LOG_ERROR("Failed to push task {} to queue", task.id);
        redis_pool_->returnConnection(redis);
        return false;
    }
    
    LOG_INFO("Task {} submitted with priority {}", task.id, task.priority);
    redis_pool_->returnConnection(redis);
    return true;
}

std::string TaskDispatcher::popNextTask() {
    auto redis = redis_pool_->getConnection();
    if (!redis) {
        LOG_ERROR("Failed to get Redis connection for task pop");
        return "";
    }
    
    std::string task_id = redis->popFromPriorityQueue();
    redis_pool_->returnConnection(redis);
    
    if (!task_id.empty()) {
        LOG_DEBUG("Popped task {} from queue", task_id);
    }
    
    return task_id;
}

bool TaskDispatcher::assignTaskToWorker(const std::string& task_id, const std::string& worker_id) {
    auto redis = redis_pool_->getConnection();
    if (!redis) {
        LOG_ERROR("Failed to get Redis connection for task assignment");
        return false;
    }
    
    TaskInfo task = redis->getTask(task_id);
    if (task.id.empty()) {
        LOG_ERROR("Task {} not found", task_id);
        redis_pool_->returnConnection(redis);
        return false;
    }
    
    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    std::string key = "task:" + task_id;
    redis->hset(key, "status", "1");
    redis->hset(key, "worker_id", worker_id);
    redis->hset(key, "started_at", std::to_string(now));
    
    redis->incrementWorkerLoad(worker_id);
    
    load_balancer_->updateWorkerLoad(worker_id, 
        redis->getWorkerLoad(worker_id));
    
    LOG_INFO("Task {} assigned to worker {}", task_id, worker_id);
    redis_pool_->returnConnection(redis);
    return true;
}

bool TaskDispatcher::completeTask(const std::string& task_id, const TaskResult& result) {
    auto redis = redis_pool_->getConnection();
    if (!redis) {
        LOG_ERROR("Failed to get Redis connection for task completion");
        return false;
    }
    
    std::string worker_id = redis->hget("task:" + task_id, "worker_id");
    
    redis->updateTaskStatus(task_id, 2);
    redis->hset("task:" + task_id, "result", result.result);
    
    if (!worker_id.empty()) {
        redis->decrementWorkerLoad(worker_id);
        load_balancer_->updateWorkerLoad(worker_id, redis->getWorkerLoad(worker_id));
        load_balancer_->recordSuccess(worker_id);
    }
    
    LOG_INFO("Task {} completed successfully", task_id);
    redis_pool_->returnConnection(redis);
    return true;
}

bool TaskDispatcher::failTask(const std::string& task_id, const std::string& error) {
    auto redis = redis_pool_->getConnection();
    if (!redis) {
        LOG_ERROR("Failed to get Redis connection for task failure");
        return false;
    }
    
    std::string worker_id = redis->hget("task:" + task_id, "worker_id");
    
    redis->updateTaskStatus(task_id, 3, error);
    
    if (!worker_id.empty()) {
        redis->decrementWorkerLoad(worker_id);
        load_balancer_->updateWorkerLoad(worker_id, redis->getWorkerLoad(worker_id));
        load_balancer_->recordFailure(worker_id);
    }
    
    LOG_ERROR("Task {} failed: {}", task_id, error);
    redis_pool_->returnConnection(redis);
    return true;
}

bool TaskDispatcher::retryTask(const std::string& task_id) {
    auto redis = redis_pool_->getConnection();
    if (!redis) {
        LOG_ERROR("Failed to get Redis connection for task retry");
        return false;
    }
    
    int retry_count = redis->getTask(task_id).retry_count;
    
    if (retry_count >= 3) {
        LOG_WARN("Task {} exceeded max retries", task_id);
        redis_pool_->returnConnection(redis);
        return false;
    }
    
    std::string worker_id = redis->hget("task:" + task_id, "worker_id");
    if (!worker_id.empty()) {
        redis->decrementWorkerLoad(worker_id);
        load_balancer_->updateWorkerLoad(worker_id, redis->getWorkerLoad(worker_id));
    }
    
    redis->incrementTaskRetry(task_id);
    TaskInfo task = redis->getTask(task_id);
    
    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    int delay_seconds = 1 << retry_count;
    
    std::string retry_key = "retry:" + std::to_string(delay_seconds) + ":" + task_id;
    redis->setex(retry_key, task_id, delay_seconds);
    redis->pushToPriorityQueue(task.priority, task_id);
    
    LOG_INFO("Task {} scheduled for retry {} (delay: {}s)", task_id, retry_count + 1, delay_seconds);
    redis_pool_->returnConnection(redis);
    return true;
}

int TaskDispatcher::getTaskRetryCount(const std::string& task_id) {
    auto redis = redis_pool_->getConnection();
    if (!redis) {
        return 0;
    }
    
    int retry_count = redis->getTask(task_id).retry_count;
    redis_pool_->returnConnection(redis);
    return retry_count;
}

std::string TaskDispatcher::getTaskStatus(const std::string& task_id) {
    auto redis = redis_pool_->getConnection();
    if (!redis) {
        return "unknown";
    }
    
    std::string status = redis->hget("task:" + task_id, "status");
    redis_pool_->returnConnection(redis);
    
    if (status.empty()) {
        return "not_found";
    }
    
    int status_code = std::stoi(status);
    switch (status_code) {
        case 0: return "pending";
        case 1: return "running";
        case 2: return "success";
        case 3: return "failed";
        default: return "unknown";
    }
}

TaskInfo TaskDispatcher::getTaskInfo(const std::string& task_id) {
    auto redis = redis_pool_->getConnection();
    if (!redis) {
        return TaskInfo{};
    }
    
    TaskInfo info = redis->getTask(task_id);
    redis_pool_->returnConnection(redis);
    return info;
}

size_t TaskDispatcher::getQueueSize(int priority) const {
    auto redis = redis_pool_->getConnection();
    if (!redis) {
        return 0;
    }
    
    std::string queue;
    switch (priority) {
        case 2: queue = "queue:high"; break;
        case 1: queue = "queue:medium"; break;
        default: queue = "queue:low"; break;
    }
    
    long long size = redis->llen(queue);
    redis_pool_->returnConnection(redis);
    return static_cast<size_t>(size);
}

size_t TaskDispatcher::getTotalQueueSize() const {
    return getQueueSize(2) + getQueueSize(1) + getQueueSize(0);
}

}
