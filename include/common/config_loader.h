#pragma once

#include <string>
#include <memory>
#include <map>
#include <yaml-cpp/yaml.h>

namespace taskflow {

class ConfigLoader {
public:
    ConfigLoader() = default;
    ~ConfigLoader() = default;
    ConfigLoader(const ConfigLoader&) = delete;
    ConfigLoader& operator=(const ConfigLoader&) = delete;
    
    bool load(const std::string& config_path);
    
    template<typename T>
    T get(const std::string& key, const T& default_value) const {
        try {
            return config_[key].as<T>();
        } catch (...) {
            return default_value;
        }
    }
    
    template<typename T>
    T getPath(const std::string& path, const T& default_value) const {
        try {
            YAML::Node node = config_;
            size_t start = 0;
            std::string key = path;
            
            while ((start = key.find('.')) != std::string::npos) {
                std::string part = key.substr(0, start);
                node = node[part];
                key = key.substr(start + 1);
            }
            
            return node[key].as<T>();
        } catch (...) {
            return default_value;
        }
    }
    
    bool hasKey(const std::string& key) const;
    const YAML::Node& getRoot() const { return config_; }
    
private:
    YAML::Node config_;
};

struct GatewayConfig {
    int port;
    std::string log_level;
    std::string scheduler_addr;
    std::string log_dir;
    
    GatewayConfig() : port(8080), log_level("info"), 
                      scheduler_addr("localhost:50051"), log_dir("./logs") {}
};

struct SchedulerConfig {
    int port;
    std::string load_balance;
    int worker_check_interval;
    int task_timeout_seconds;
    std::string etcd_endpoints;
    std::string redis_addr;
    std::string log_level;
    std::string log_dir;
    
    SchedulerConfig() : port(50051), load_balance("least_connection"),
                        worker_check_interval(5), task_timeout_seconds(60),
                        etcd_endpoints("localhost:2379"), redis_addr("localhost:6379"),
                        log_level("info"), log_dir("./logs") {}
};

struct WorkerConfig {
    int id;
    int port;
    std::string address;
    std::string scheduler_addr;
    std::string etcd_endpoints;
    int heartbeat_interval;
    int max_concurrent_tasks;
    std::string log_level;
    std::string log_dir;
    
    WorkerConfig() : id(1), port(50052), address("localhost"),
                     scheduler_addr("localhost:50051"), etcd_endpoints("localhost:2379"),
                     heartbeat_interval(5), max_concurrent_tasks(10),
                     log_level("info"), log_dir("./logs") {}
};

struct TrackerConfig {
    int port;
    std::string redis_addr;
    std::string log_level;
    std::string log_dir;
    
    TrackerConfig() : port(50053), redis_addr("localhost:6379"),
                      log_level("info"), log_dir("./logs") {}
};

struct StatsConfig {
    int port;
    std::string redis_addr;
    int collection_interval;
    std::string log_level;
    std::string log_dir;
    
    StatsConfig() : port(50054), redis_addr("localhost:6379"),
                    collection_interval(10), log_level("info"), log_dir("./logs") {}
};

GatewayConfig loadGatewayConfig(const std::string& config_path);
SchedulerConfig loadSchedulerConfig(const std::string& config_path);
WorkerConfig loadWorkerConfig(const std::string& config_path);
TrackerConfig loadTrackerConfig(const std::string& config_path);
StatsConfig loadStatsConfig(const std::string& config_path);

}
