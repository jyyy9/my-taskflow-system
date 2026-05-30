#include "common/config_loader.h"
#include "common/logger.h"
#include <fstream>

namespace taskflow {

ConfigLoader& ConfigLoader::instance() {
    static ConfigLoader instance;
    return instance;
}

bool ConfigLoader::load(const std::string& config_path) {
    try {
        config_ = YAML::LoadFile(config_path);
        LOG_INFO("Configuration loaded from: {}", config_path);
        return true;
    } catch (const YAML::Exception& e) {
        LOG_ERROR("Failed to load config from {}: {}", config_path, e.what());
        return false;
    }
}

bool ConfigLoader::hasKey(const std::string& key) const {
    try {
        YAML::Node node = config_;
        size_t start = 0;
        std::string remaining = key;
        
        while ((start = remaining.find('.')) != std::string::npos) {
            std::string part = remaining.substr(0, start);
            if (!node[part]) return false;
            node = node[part];
            remaining = remaining.substr(start + 1);
        }
        
        return !!node[remaining];
    } catch (...) {
        return false;
    }
}

GatewayConfig loadGatewayConfig(const std::string& config_path) {
    GatewayConfig config;
    ConfigLoader loader;
    
    if (!loader.load(config_path)) {
        LOG_WARN("Using default GatewayConfig");
        return config;
    }
    
    config.port = loader.getPath<int>("gateway.port", 8080);
    config.log_level = loader.getPath<std::string>("gateway.log_level", "info");
    config.scheduler_addr = loader.getPath<std::string>("gateway.scheduler_addr", "localhost:50051");
    config.log_dir = loader.getPath<std::string>("gateway.log_dir", "./logs");
    
    return config;
}

SchedulerConfig loadSchedulerConfig(const std::string& config_path) {
    SchedulerConfig config;
    ConfigLoader loader;
    
    if (!loader.load(config_path)) {
        LOG_WARN("Using default SchedulerConfig");
        return config;
    }
    
    config.port = loader.getPath<int>("scheduler.port", 50051);
    config.load_balance = loader.getPath<std::string>("scheduler.load_balance", "least_connection");
    config.worker_check_interval = loader.getPath<int>("scheduler.worker_check_interval", 5);
    config.task_timeout_seconds = loader.getPath<int>("scheduler.task_timeout_seconds", 60);
    config.etcd_endpoints = loader.getPath<std::string>("scheduler.etcd_endpoints", "localhost:2379");
    config.redis_addr = loader.getPath<std::string>("scheduler.redis_addr", "localhost:6379");
    config.log_level = loader.getPath<std::string>("scheduler.log_level", "info");
    config.log_dir = loader.getPath<std::string>("scheduler.log_dir", "./logs");
    
    return config;
}

WorkerConfig loadWorkerConfig(const std::string& config_path) {
    WorkerConfig config;
    ConfigLoader loader;
    
    if (!loader.load(config_path)) {
        LOG_WARN("Using default WorkerConfig");
        return config;
    }
    
    config.id = loader.getPath<int>("worker.id", 1);
    config.port = loader.getPath<int>("worker.port", 50052);
    config.address = loader.getPath<std::string>("worker.address", "localhost");
    config.scheduler_addr = loader.getPath<std::string>("worker.scheduler_addr", "localhost:50051");
    config.etcd_endpoints = loader.getPath<std::string>("worker.etcd_endpoints", "localhost:2379");
    config.heartbeat_interval = loader.getPath<int>("worker.heartbeat_interval", 5);
    config.max_concurrent_tasks = loader.getPath<int>("worker.max_concurrent_tasks", 10);
    config.log_level = loader.getPath<std::string>("worker.log_level", "info");
    config.log_dir = loader.getPath<std::string>("worker.log_dir", "./logs");
    
    return config;
}

TrackerConfig loadTrackerConfig(const std::string& config_path) {
    TrackerConfig config;
    ConfigLoader loader;
    
    if (!loader.load(config_path)) {
        LOG_WARN("Using default TrackerConfig");
        return config;
    }
    
    config.port = loader.getPath<int>("tracker.port", 50053);
    config.redis_addr = loader.getPath<std::string>("tracker.redis_addr", "localhost:6379");
    config.log_level = loader.getPath<std::string>("tracker.log_level", "info");
    config.log_dir = loader.getPath<std::string>("tracker.log_dir", "./logs");
    
    return config;
}

StatsConfig loadStatsConfig(const std::string& config_path) {
    StatsConfig config;
    ConfigLoader loader;
    
    if (!loader.load(config_path)) {
        LOG_WARN("Using default StatsConfig");
        return config;
    }
    
    config.port = loader.getPath<int>("stats.port", 50054);
    config.redis_addr = loader.getPath<std::string>("stats.redis_addr", "localhost:6379");
    config.collection_interval = loader.getPath<int>("stats.collection_interval", 10);
    config.log_level = loader.getPath<std::string>("stats.log_level", "info");
    config.log_dir = loader.getPath<std::string>("stats.log_dir", "./logs");
    
    return config;
}

}
