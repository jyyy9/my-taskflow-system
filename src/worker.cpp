#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
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

class WorkerServiceImpl final : public WorkerService::Service {
public:
    WorkerServiceImpl(const taskflow::WorkerConfig& config)
        : config_(config), current_load_(0) {
    }
    
    grpc::Status ExecuteTask(grpc::ServerContext* context,
                             const ExecuteTaskRequest* request,
                             ExecuteTaskReply* reply) override {
        std::string task_id = request->task_id();
        int task_type = request->type();
        std::string data = request->data();
        int timeout = request->timeout_seconds();
        
        LOG_INFO("Worker {} executing task {} (type: {})", config_.id, task_id, task_type);
        
        current_load_++;
        
        std::string result;
        bool success = true;
        std::string error_message;
        
        try {
            result = executeTaskInternal(task_type, data, timeout);
        } catch (const std::exception& e) {
            success = false;
            error_message = e.what();
            LOG_ERROR("Task {} execution failed: {}", task_id, e.what());
        }
        
        current_load_--;
        
        reply->set_success(success);
        reply->set_task_id(task_id);
        reply->set_result(result);
        reply->set_error_message(error_message);
        
        return grpc::Status::OK;
    }
    
    grpc::Status Heartbeat(grpc::ServerContext* context,
                          const HeartbeatRequest* request,
                          HeartbeatReply* reply) override {
        LOG_DEBUG("Heartbeat acknowledged");
        reply->set_success(true);
        reply->set_message("OK");
        return grpc::Status::OK;
    }

private:
    std::string executeTaskInternal(int task_type, const std::string& data, int timeout) {
        LOG_DEBUG("Starting task execution, type: {}, timeout: {}", task_type, timeout);
        
        switch (task_type) {
            case 0:
                return simulateOrderDispatch(data, timeout);
            case 1:
                return simulatePersonnelSchedule(data, timeout);
            case 2:
                return simulateMessageNotify(data, timeout);
            case 3:
                return simulateDataStats(data, timeout);
            default:
                return simulateGenericTask(data, timeout);
        }
    }
    
    std::string simulateOrderDispatch(const std::string& order_data, int timeout) {
        LOG_INFO("Simulating order dispatch for: {}", order_data);
        
        int duration = std::min(timeout, 5);
        std::this_thread::sleep_for(std::chrono::seconds(duration));
        
        std::ostringstream result;
        result << "Order dispatched successfully. Order: " << order_data 
               << ". Driver assigned: D-" << (rand() % 1000 + 1);
        return result.str();
    }
    
    std::string simulatePersonnelSchedule(const std::string& personnel_data, int timeout) {
        LOG_INFO("Simulating personnel scheduling for: {}", personnel_data);
        
        int duration = std::min(timeout, 8);
        std::this_thread::sleep_for(std::chrono::seconds(duration));
        
        std::ostringstream result;
        result << "Personnel scheduling completed. Personnel: " << personnel_data 
               << ". Schedule generated for next 7 days.";
        return result.str();
    }
    
    std::string simulateMessageNotify(const std::string& message_data, int timeout) {
        LOG_INFO("Simulating message notification for: {}", message_data);
        
        int duration = std::min(timeout, 2);
        std::this_thread::sleep_for(std::chrono::seconds(duration));
        
        std::ostringstream result;
        result << "Message notification sent. Recipients: " << (rand() % 500 + 50)
               << ". Delivery rate: 98%";
        return result.str();
    }
    
    std::string simulateDataStats(const std::string& stats_data, int timeout) {
        LOG_INFO("Simulating data statistics for: {}", stats_data);
        
        int duration = std::min(timeout, 10);
        std::this_thread::sleep_for(std::chrono::seconds(duration));
        
        std::ostringstream result;
        result << "Data statistics completed. Period: " << stats_data
               << ". Total orders: " << (rand() % 10000 + 5000)
               << ". Total revenue: ¥" << (rand() % 1000000 + 100000);
        return result.str();
    }
    
    std::string simulateGenericTask(const std::string& data, int timeout) {
        LOG_INFO("Simulating generic task with data: {}", data);
        
        int duration = std::min(timeout, 3);
        std::this_thread::sleep_for(std::chrono::seconds(duration));
        
        std::ostringstream result;
        result << "Task completed. Processed data: " << data;
        return result.str();
    }

private:
    taskflow::WorkerConfig config_;
    std::atomic<int> current_load_;
};

void heartbeatLoop(const taskflow::WorkerConfig& config, 
                   std::shared_ptr<taskflow::RedisClient> redis) {
    std::string worker_id = "worker-" + std::to_string(config.id);
    
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(config.heartbeat_interval));
        
        if (!g_running) break;
        
        redis->setex("worker:heartbeat:" + worker_id, 
                    std::to_string(std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count()),
                    config.heartbeat_interval * 2);
        
        LOG_DEBUG("Heartbeat sent for worker {}", worker_id);
    }
}

void registerWithScheduler(const taskflow::WorkerConfig& config) {
    std::string worker_id = "worker-" + std::to_string(config.id);
    
    LOG_INFO("Registering worker {} at {}:{}", worker_id, config.address, config.port);
    
    taskflow::ServiceRegistry::instance().init(config.etcd_endpoints);
    
    if (!taskflow::ServiceRegistry::instance().registerService(
            "worker", worker_id, config.address, config.port,
            {{"max_load", std::to_string(config.max_concurrent_tasks)}})) {
        LOG_ERROR("Failed to register worker with etcd");
    } else {
        taskflow::ServiceRegistry::instance().startRefreshLoop("worker", worker_id, 5);
        LOG_INFO("Worker {} registered successfully", worker_id);
    }
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    std::string config_path = "config/worker.yaml";
    taskflow::WorkerConfig config;
    
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg.find("--id=") == 0) {
            config.id = std::stoi(arg.substr(5));
        } else if (arg.find("--config=") == 0) {
            config_path = arg.substr(10);
        }
    }
    
    if (config_path != "" && config_path.find("--") == std::string::npos) {
        config = taskflow::loadWorkerConfig(config_path);
    }
    
    if (config.id == 0) {
        for (int i = 1; i < argc; i++) {
            std::string arg = argv[i];
            if (arg.find("--id=") == 0) {
                config.id = std::stoi(arg.substr(5));
                break;
            }
        }
        if (config.id == 0) {
            config.id = 1;
        }
    }
    
    taskflow::Logger::instance().init("worker-" + std::to_string(config.id), config.log_dir);
    taskflow::Logger::instance().setLevel(config.log_level);
    
    LOG_INFO("Starting TaskFlow Worker {}...", config.id);
    LOG_INFO("Config - Port: {}, Scheduler: {}", config.port, config.scheduler_addr);
    
    registerWithScheduler(config);
    
    std::vector<std::string> redis_parts;
    std::istringstream redis_stream(config.etcd_endpoints);
    std::string redis_host = "localhost";
    int redis_port = 6379;
    
    auto redis = std::make_shared<taskflow::RedisClient>(redis_host, redis_port);
    redis->connect();
    
    std::thread heartbeat_thread(heartbeatLoop, config, redis);
    
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    
    std::string addr = "0.0.0.0:" + std::to_string(config.port);
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    
    auto worker_service = std::make_shared<WorkerServiceImpl>(config);
    builder.RegisterService(worker_service.get());
    
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    
    LOG_INFO("Worker {} listening on {}", config.id, addr);
    
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    LOG_INFO("Worker {} shutting down...", config.id);
    
    taskflow::ServiceRegistry::instance().shutdown();
    
    g_running = false;
    
    if (heartbeat_thread.joinable()) {
        heartbeat_thread.join();
    }
    
    server->Shutdown();
    
    LOG_INFO("Worker {} stopped", config.id);
    return 0;
}
