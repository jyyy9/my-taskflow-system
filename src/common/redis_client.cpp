#include "common/redis_client.h"
#include "common/logger.h"
#include <sstream>
#include <chrono>

namespace taskflow {

static const std::string TASK_PREFIX = "task:";
static const std::string QUEUE_HIGH = "queue:high";
static const std::string QUEUE_MEDIUM = "queue:medium";
static const std::string QUEUE_LOW = "queue:low";
static const std::string WORKER_LOAD_PREFIX = "worker:load:";

RedisClient::RedisClient(const std::string& host, int port)
    : host_(host), port_(port), context_(nullptr), connected_(false) {
}

RedisClient::~RedisClient() {
    disconnect();
}

bool RedisClient::connect() {
    if (connected_ && context_) {
        return true;
    }
    
    struct timeval timeout = {2, 0};
    context_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);
    
    if (context_ == nullptr || context_->err) {
        if (context_) {
            LOG_ERROR("Redis connection error: {}", context_->errstr);
            redisFree(context_);
            context_ = nullptr;
        } else {
            LOG_ERROR("Redis connection error: can't allocate redis context");
        }
        connected_ = false;
        return false;
    }
    
    connected_ = true;
    LOG_INFO("Connected to Redis at {}:{}", host_, port_);
    return true;
}

void RedisClient::disconnect() {
    if (context_) {
        redisFree(context_);
        context_ = nullptr;
    }
    connected_ = false;
}

bool RedisClient::set(const std::string& key, const std::string& value) {
    if (!connected_) return false;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "SET %s %s", key.c_str(), value.c_str());
    if (!reply) return false;
    
    bool success = reply->type != REDIS_REPLY_ERROR;
    freeReplyObject(reply);
    return success;
}

bool RedisClient::setex(const std::string& key, const std::string& value, int seconds) {
    if (!connected_) return false;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "SETEX %s %d %s", key.c_str(), seconds, value.c_str());
    if (!reply) return false;
    
    bool success = reply->type != REDIS_REPLY_ERROR;
    freeReplyObject(reply);
    return success;
}

std::string RedisClient::get(const std::string& key) {
    if (!connected_) return "";
    
    redisReply* reply = (redisReply*)redisCommand(context_, "GET %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_STRING) {
        if (reply) freeReplyObject(reply);
        return "";
    }
    
    std::string result(reply->str, reply->len);
    freeReplyObject(reply);
    return result;
}

bool RedisClient::del(const std::string& key) {
    if (!connected_) return false;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "DEL %s", key.c_str());
    if (!reply) return false;
    
    bool success = reply->type != REDIS_REPLY_INTEGER && reply->integer > 0;
    freeReplyObject(reply);
    return success;
}

bool RedisClient::exists(const std::string& key) {
    if (!connected_) return false;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "EXISTS %s", key.c_str());
    if (!reply) return false;
    
    bool exists = reply->type == REDIS_REPLY_INTEGER && reply->integer > 0;
    freeReplyObject(reply);
    return exists;
}

bool RedisClient::lpush(const std::string& key, const std::string& value) {
    if (!connected_) return false;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "LPUSH %s %s", key.c_str(), value.c_str());
    if (!reply) return false;
    
    bool success = reply->type != REDIS_REPLY_ERROR;
    freeReplyObject(reply);
    return success;
}

bool RedisClient::rpush(const std::string& key, const std::string& value) {
    if (!connected_) return false;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "RPUSH %s %s", key.c_str(), value.c_str());
    if (!reply) return false;
    
    bool success = reply->type != REDIS_REPLY_ERROR;
    freeReplyObject(reply);
    return success;
}

std::string RedisClient::lpop(const std::string& key) {
    if (!connected_) return "";
    
    redisReply* reply = (redisReply*)redisCommand(context_, "LPOP %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_STRING) {
        if (reply) freeReplyObject(reply);
        return "";
    }
    
    std::string result(reply->str, reply->len);
    freeReplyObject(reply);
    return result;
}

std::string RedisClient::rpop(const std::string& key) {
    if (!connected_) return "";
    
    redisReply* reply = (redisReply*)redisCommand(context_, "RPOP %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_STRING) {
        if (reply) freeReplyObject(reply);
        return "";
    }
    
    std::string result(reply->str, reply->len);
    freeReplyObject(reply);
    return result;
}

std::vector<std::string> RedisClient::lrange(const std::string& key, int start, int stop) {
    std::vector<std::string> result;
    if (!connected_) return result;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "LRANGE %s %d %d", key.c_str(), start, stop);
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
        if (reply) freeReplyObject(reply);
        return result;
    }
    
    for (size_t i = 0; i < reply->elements; i++) {
        if (reply->element[i]->type == REDIS_REPLY_STRING) {
            result.emplace_back(reply->element[i]->str, reply->element[i]->len);
        }
    }
    freeReplyObject(reply);
    return result;
}

long long RedisClient::llen(const std::string& key) {
    if (!connected_) return 0;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "LLEN %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        if (reply) freeReplyObject(reply);
        return 0;
    }
    
    long long count = reply->integer;
    freeReplyObject(reply);
    return count;
}

bool RedisClient::hset(const std::string& key, const std::string& field, const std::string& value) {
    if (!connected_) return false;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());
    if (!reply) return false;
    
    bool success = reply->type != REDIS_REPLY_ERROR;
    freeReplyObject(reply);
    return success;
}

std::string RedisClient::hget(const std::string& key, const std::string& field) {
    if (!connected_) return "";
    
    redisReply* reply = (redisReply*)redisCommand(context_, "HGET %s %s", key.c_str(), field.c_str());
    if (!reply || reply->type != REDIS_REPLY_STRING) {
        if (reply) freeReplyObject(reply);
        return "";
    }
    
    std::string result(reply->str, reply->len);
    freeReplyObject(reply);
    return result;
}

bool RedisClient::hmset(const std::string& key, const std::map<std::string, std::string>& fields) {
    if (!connected_ || fields.empty()) return false;
    
    std::vector<std::string> args;
    args.push_back("HMSET");
    args.push_back(key);
    for (const auto& pair : fields) {
        args.push_back(pair.first);
        args.push_back(pair.second);
    }
    
    std::vector<const char*> argv;
    std::vector<size_t> argc;
    for (const auto& arg : args) {
        argv.push_back(arg.c_str());
        argc.push_back(arg.size());
    }
    
    redisReply* reply = (redisReply*)redisCommandArgv(context_, argv.size(), argv.data(), argc.data());
    if (!reply) return false;
    
    bool success = reply->type != REDIS_REPLY_ERROR;
    freeReplyObject(reply);
    return success;
}

std::map<std::string, std::string> RedisClient::hgetall(const std::string& key) {
    std::map<std::string, std::string> result;
    if (!connected_) return result;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "HGETALL %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_ARRAY) {
        if (reply) freeReplyObject(reply);
        return result;
    }
    
    for (size_t i = 0; i + 1 < reply->elements; i += 2) {
        if (reply->element[i]->type == REDIS_REPLY_STRING && 
            reply->element[i + 1]->type == REDIS_REPLY_STRING) {
            result[std::string(reply->element[i]->str, reply->element[i]->len)] = 
                std::string(reply->element[i + 1]->str, reply->element[i + 1]->len);
        }
    }
    freeReplyObject(reply);
    return result;
}

bool RedisClient::hdel(const std::string& key, const std::string& field) {
    if (!connected_) return false;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "HDEL %s %s", key.c_str(), field.c_str());
    if (!reply) return false;
    
    bool success = reply->type != REDIS_REPLY_ERROR;
    freeReplyObject(reply);
    return success;
}

bool RedisClient::hexists(const std::string& key, const std::string& field) {
    if (!connected_) return false;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "HEXISTS %s %s", key.c_str(), field.c_str());
    if (!reply) return false;
    
    bool exists = reply->type == REDIS_REPLY_INTEGER && reply->integer > 0;
    freeReplyObject(reply);
    return exists;
}

long long RedisClient::incr(const std::string& key) {
    if (!connected_) return 0;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "INCR %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        if (reply) freeReplyObject(reply);
        return 0;
    }
    
    long long result = reply->integer;
    freeReplyObject(reply);
    return result;
}

long long RedisClient::incrby(const std::string& key, long long increment) {
    if (!connected_) return 0;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "INCRBY %s %lld", key.c_str(), increment);
    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        if (reply) freeReplyObject(reply);
        return 0;
    }
    
    long long result = reply->integer;
    freeReplyObject(reply);
    return result;
}

bool RedisClient::expire(const std::string& key, int seconds) {
    if (!connected_) return false;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "EXPIRE %s %d", key.c_str(), seconds);
    if (!reply) return false;
    
    bool success = reply->type == REDIS_REPLY_INTEGER && reply->integer > 0;
    freeReplyObject(reply);
    return success;
}

long long RedisClient::ttl(const std::string& key) {
    if (!connected_) return -1;
    
    redisReply* reply = (redisReply*)redisCommand(context_, "TTL %s", key.c_str());
    if (!reply || reply->type != REDIS_REPLY_INTEGER) {
        if (reply) freeReplyObject(reply);
        return -1;
    }
    
    long long result = reply->integer;
    freeReplyObject(reply);
    return result;
}

TaskInfo RedisClient::getTask(const std::string& task_id) {
    TaskInfo task;
    task.id = task_id;
    
    std::string key = TASK_PREFIX + task_id;
    auto fields = hgetall(key);
    
    if (fields.empty()) {
        return task;
    }
    
    auto getInt = [&](const std::string& field) -> int64_t {
        auto it = fields.find(field);
        if (it != fields.end()) {
            return std::stoll(it->second);
        }
        return 0;
    };
    
    task.type = getInt("type");
    task.priority = getInt("priority");
    task.data = fields.count("data") ? fields["data"] : "";
    task.created_at = getInt("created_at");
    task.started_at = getInt("started_at");
    task.finished_at = getInt("finished_at");
    task.status = getInt("status");
    task.retry_count = getInt("retry_count");
    task.error_message = fields.count("error_message") ? fields["error_message"] : "";
    task.result = fields.count("result") ? fields["result"] : "";
    
    return task;
}

bool RedisClient::saveTask(const TaskInfo& task) {
    std::string key = TASK_PREFIX + task.id;
    
    std::map<std::string, std::string> fields = {
        {"type", std::to_string(task.type)},
        {"priority", std::to_string(task.priority)},
        {"data", task.data},
        {"created_at", std::to_string(task.created_at)},
        {"started_at", std::to_string(task.started_at)},
        {"finished_at", std::to_string(task.finished_at)},
        {"status", std::to_string(task.status)},
        {"retry_count", std::to_string(task.retry_count)},
        {"error_message", task.error_message},
        {"result", task.result}
    };
    
    return hmset(key, fields);
}

bool RedisClient::updateTaskStatus(const std::string& task_id, int status, const std::string& error_msg) {
    std::string key = TASK_PREFIX + task_id;
    
    hset(key, "status", std::to_string(status));
    
    if (!error_msg.empty()) {
        hset(key, "error_message", error_msg);
    }
    
    if (status == 2 || status == 3) {
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        hset(key, "finished_at", std::to_string(now));
    }
    
    return true;
}

bool RedisClient::incrementTaskRetry(const std::string& task_id) {
    std::string key = TASK_PREFIX + task_id;
    std::string current = hget(key, "retry_count");
    int retry = current.empty() ? 0 : std::stoi(current);
    return hset(key, "retry_count", std::to_string(retry + 1));
}

bool RedisClient::pushToPriorityQueue(int priority, const std::string& task_id) {
    std::string queue;
    switch (priority) {
        case 2: queue = QUEUE_HIGH; break;
        case 1: queue = QUEUE_MEDIUM; break;
        default: queue = QUEUE_LOW; break;
    }
    return rpush(queue, task_id);
}

std::string RedisClient::popFromPriorityQueue() {
    std::string task_id = lpop(QUEUE_HIGH);
    if (!task_id.empty()) return task_id;
    
    task_id = lpop(QUEUE_MEDIUM);
    if (!task_id.empty()) return task_id;
    
    return lpop(QUEUE_LOW);
}

bool RedisClient::incrementWorkerLoad(const std::string& worker_id) {
    std::string key = WORKER_LOAD_PREFIX + worker_id;
    incr(key);
    return true;
}

bool RedisClient::decrementWorkerLoad(const std::string& worker_id) {
    std::string key = WORKER_LOAD_PREFIX + worker_id;
    long long load = std::stoll(get(key));
    if (load > 0) {
        incrby(key, -1);
    }
    return true;
}

int RedisClient::getWorkerLoad(const std::string& worker_id) {
    std::string key = WORKER_LOAD_PREFIX + worker_id;
    std::string load = get(key);
    return load.empty() ? 0 : std::stoi(load);
}

RedisPool::RedisPool(const std::string& host, int port, size_t pool_size)
    : host_(host), port_(port), pool_size_(pool_size) {
    
    for (size_t i = 0; i < pool_size_; i++) {
        auto client = std::make_shared<RedisClient>(host, port);
        if (client->connect()) {
            pool_.push_back(client);
            in_use_.push_back(false);
        }
    }
    
    LOG_INFO("Redis pool created with {} connections", pool_.size());
}

RedisPool::~RedisPool() {
    std::lock_guard<std::mutex> lock(mutex_);
    pool_.clear();
}

std::shared_ptr<RedisClient> RedisPool::getConnection() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (size_t i = 0; i < pool_.size(); i++) {
        if (!in_use_[i]) {
            if (pool_[i]->isConnected() || pool_[i]->connect()) {
                in_use_[i] = true;
                return pool_[i];
            }
        }
    }
    
    auto client = std::make_shared<RedisClient>(host_, port_);
    if (client->connect()) {
        pool_.push_back(client);
        in_use_.push_back(true);
        LOG_INFO("Created new Redis connection in pool");
        return client;
    }
    
    return nullptr;
}

void RedisPool::returnConnection(std::shared_ptr<RedisClient> conn) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (size_t i = 0; i < pool_.size(); i++) {
        if (pool_[i] == conn) {
            in_use_[i] = false;
            return;
        }
    }
}

size_t RedisPool::getActiveConnections() const {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    for (bool in_use : in_use_) {
        if (in_use) count++;
    }
    return count;
}

}
