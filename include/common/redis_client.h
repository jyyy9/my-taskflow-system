#pragma once

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <functional>
#include <hiredis/hiredis.h>
#include <boost/asio.hpp>

namespace taskflow {

struct TaskInfo {
    std::string id;
    int type;
    int priority;
    std::string data;
    int64_t created_at;
    int64_t started_at;
    int64_t finished_at;
    int status;
    int retry_count;
    std::string error_message;
    std::string result;
};

class RedisClient {
public:
    RedisClient(const std::string& host, int port);
    ~RedisClient();
    
    bool connect();
    void disconnect();
    bool isConnected() const { return connected_; }
    
    bool set(const std::string& key, const std::string& value);
    bool setex(const std::string& key, const std::string& value, int seconds);
    std::string get(const std::string& key);
    bool del(const std::string& key);
    bool exists(const std::string& key);
    
    bool lpush(const std::string& key, const std::string& value);
    bool rpush(const std::string& key, const std::string& value);
    std::string lpop(const std::string& key);
    std::string rpop(const std::string& key);
    std::vector<std::string> lrange(const std::string& key, int start, int stop);
    long long llen(const std::string& key);
    
    bool hset(const std::string& key, const std::string& field, const std::string& value);
    std::string hget(const std::string& key, const std::string& field);
    bool hmset(const std::string& key, const std::map<std::string, std::string>& fields);
    std::map<std::string, std::string> hgetall(const std::string& key);
    bool hdel(const std::string& key, const std::string& field);
    bool hexists(const std::string& key, const std::string& field);
    
    long long incr(const std::string& key);
    long long incrby(const std::string& key, long long increment);
    
    bool expire(const std::string& key, int seconds);
    long long ttl(const std::string& key);
    
    TaskInfo getTask(const std::string& task_id);
    bool saveTask(const TaskInfo& task);
    bool updateTaskStatus(const std::string& task_id, int status, const std::string& error_msg = "");
    bool incrementTaskRetry(const std::string& task_id);
    
    bool pushToPriorityQueue(int priority, const std::string& task_id);
    std::string popFromPriorityQueue();
    
    bool incrementWorkerLoad(const std::string& worker_id);
    bool decrementWorkerLoad(const std::string& worker_id);
    int getWorkerLoad(const std::string& worker_id);
    
private:
    std::string host_;
    int port_;
    redisContext* context_;
    bool connected_;
};

class RedisPool {
public:
    RedisPool(const std::string& host, int port, size_t pool_size = 4);
    ~RedisPool();
    
    std::shared_ptr<RedisClient> getConnection();
    void returnConnection(std::shared_ptr<RedisClient> conn);
    
    size_t getPoolSize() const { return pool_size_; }
    size_t getActiveConnections() const;

private:
    std::string host_;
    int port_;
    size_t pool_size_;
    std::vector<std::shared_ptr<RedisClient>> pool_;
    std::vector<bool> in_use_;
    mutable std::mutex mutex_;
};

}
