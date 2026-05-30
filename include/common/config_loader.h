#pragma once

#include <string>
#include <memory>
#include <map>
#include <yaml-cpp/yaml.h>

namespace taskflow {

class ConfigLoader {
public:
    static ConfigLoader& instance();
    
    bool load(const std::string& config_path);
    
    template<typename T>
    T get(const std::string& key, const T& default_value = T()) const {
        try {
            return config_[key].as<T>();
        } catch (...) {
            return default_value;
        }
    }
    
    template<typename T>
    T getPath(const std::string& path, const T& default_value = T()) const {
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
    ConfigLoader() = default;
    ~ConfigLoader() = default;
    ConfigLoader(const ConfigLoader&) = delete;
    ConfigLoader& operator=(const ConfigLoader&) = delete;
    
    YAML::Node config_;
};

struct GatewayConfig {
    int port = 8080;
    std::string log_level = "info";
    std::string scheduler_addr = "localhost:50051";
    std::string log_dir = "./logs";
};

struct SchedulerConfig {
    int port = 50051;
    std::string load_balance = "least_connection";
    int worker_check_interval = 5;
    int task_timeout_seconds = 60;
    std::string etcd_endpoints = "localhost:2379";
    std::string redis_addr = "localhost:6379";
    std::string log_level = "info";
    std::string log_dir = "./logs";
};

struct WorkerConfig {
    int id = 1;
    int port = 50052;
    std::string address = "localhost";
    std::string scheduler_addr = "localhost:50051";
    std::string etcd_endpoints = "localhost:2379";
    int heartbeat_interval = 5;
    int max_concurrent_tasks = 10;
    std::string log_level = "info";
    std::string log_dir = "./logs";
};

struct TrackerConfig {
    int port = 50053;
    std::string redis_addr = "localhost:6379";
    std::string log_level = "info";
    std::string log_dir = "./logs";
};

struct StatsConfig {
    int port = 50054;
    std::string redis_addr = "localhost:6379";
    int collection_interval = 10;
    std::string log_level = "info";
    std::string log_dir = "./logs";
};

GatewayConfig loadGatewayConfig(const std::string& config_path);
SchedulerConfig loadSchedulerConfig(const std::string& config_path);
WorkerConfig loadWorkerConfig(const std::string& config_path);
TrackerConfig loadTrackerConfig(const std::string& config_path);
StatsConfig loadStatsConfig(const std::string& config_path);

}
