#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <csignal>
#include <grpcpp/grpcpp.h>
#include "proto/taskflow.grpc.pb.h"
#include "common/logger.h"
#include "common/config_loader.h"
#include "common/redis_client.h"
#include "common/etcd_client.h"

namespace {
    std::atomic<bool> g_running(true);
}

void signalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        LOG_INFO("Received shutdown signal");
        g_running = false;
    }
}

class StatsServiceImpl final : public taskflow::StatsService::Service {
public:
    StatsServiceImpl(std::shared_ptr<taskflow::RedisClient> redis,
                    const std::string& etcd_endpoints)
        : redis_(redis), etcd_endpoints_(etcd_endpoints) {
    }
    
    grpc::Status GetStats(grpc::ServerContext* context,
                         const taskflow::GetStatsRequest* request,
                         taskflow::GetStatsReply* reply) override {
        
        long long total_tasks = 0;
        long long pending_tasks = redis_->llen("queue:high") + 
                                   redis_->llen("queue:medium") + 
                                   redis_->llen("queue:low");
        
        reply->set_total_tasks(total_tasks);
        reply->set_pending_tasks(pending_tasks);
        reply->set_running_tasks(0);
        reply->set_success_tasks(0);
        reply->set_failed_tasks(0);
        reply->set_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        
        std::vector<taskflow::ServiceInfo> workers = 
            taskflow::ServiceRegistry::instance().getServices("worker");
        
        for (const auto& worker : workers) {
            taskflow::WorkerStats* stats = reply->add_worker_stats();
            stats->set_worker_id(worker.id);
            stats->set_total_tasks(0);
            stats->set_success_tasks(0);
            stats->set_failed_tasks(0);
            stats->set_avg_execution_time_ms(0);
        }
        
        return grpc::Status::OK;
    }

private:
    std::shared_ptr<taskflow::RedisClient> redis_;
    std::string etcd_endpoints_;
};

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    std::string config_path = "config/stats.yaml";
    if (argc > 1) {
        config_path = argv[1];
    }
    
    taskflow::StatsConfig config = taskflow::loadStatsConfig(config_path);
    
    taskflow::Logger::instance().init("stats", config.log_dir);
    taskflow::Logger::instance().setLevel(config.log_level);
    
    LOG_INFO("Starting TaskFlow Stats Service...");
    LOG_INFO("Config - Port: {}, Redis: {}", config.port, config.redis_addr);
    
    taskflow::ServiceRegistry::instance().init(config.etcd_endpoints);
    
    auto redis = std::make_shared<taskflow::RedisClient>("localhost", 6379);
    if (!redis->connect()) {
        LOG_ERROR("Failed to connect to Redis");
        return 1;
    }
    
    if (!taskflow::ServiceRegistry::instance().registerService(
            "stats", "stats-1", "localhost", config.port, {})) {
        LOG_WARN("Failed to register stats service with etcd");
    } else {
        taskflow::ServiceRegistry::instance().startRefreshLoop("stats", "stats-1", 5);
    }
    
    grpc::EnableDefaultHealthCheckService(true);
    
    std::string addr = "0.0.0.0:" + std::to_string(config.port);
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    
    auto stats_service = std::make_shared<StatsServiceImpl>(redis, config.redis_addr);
    builder.RegisterService(stats_service.get());
    
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    
    LOG_INFO("Stats service listening on {}", addr);
    
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    LOG_INFO("Stats service shutting down...");
    
    taskflow::ServiceRegistry::instance().shutdown();
    
    g_running = false;
    server->Shutdown();
    
    LOG_INFO("Stats service stopped");
    return 0;
}
