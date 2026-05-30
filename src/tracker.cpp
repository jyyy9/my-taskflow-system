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

namespace {
    std::atomic<bool> g_running(true);
}

void signalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        LOG_INFO("Received shutdown signal");
        g_running = false;
    }
}

class TrackerServiceImpl final : public TrackerService::Service {
public:
    TrackerServiceImpl(std::shared_ptr<taskflow::RedisClient> redis)
        : redis_(redis) {
    }
    
    grpc::Status QueryTask(grpc::ServerContext* context,
                           const QueryTaskRequest* request,
                           QueryTaskReply* reply) override {
        std::string task_id = request->task_id();
        
        taskflow::TaskInfo task = redis_->getTask(task_id);
        
        if (task.id.empty()) {
            reply->set_success(false);
            reply->set_message("Task not found");
        } else {
            reply->set_success(true);
            reply->set_message("Task found");
            
            Task* task_msg = reply->mutable_task();
            task_msg->set_id(task.id);
            task_msg->set_type(static_cast<TaskType>(task.type));
            task_msg->set_priority(static_cast<TaskPriority>(task.priority));
            task_msg->set_data(task.data);
            task_msg->set_created_at(task.created_at);
            task_msg->set_started_at(task.started_at);
            task_msg->set_finished_at(task.finished_at);
            task_msg->set_status(static_cast<TaskStatus>(task.status));
            task_msg->set_retry_count(task.retry_count);
            task_msg->set_error_message(task.error_message);
            task_msg->set_result(task.result);
        }
        
        return grpc::Status::OK;
    }

private:
    std::shared_ptr<taskflow::RedisClient> redis_;
};

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    std::string config_path = "config/tracker.yaml";
    if (argc > 1) {
        config_path = argv[1];
    }
    
    taskflow::TrackerConfig config = taskflow::loadTrackerConfig(config_path);
    
    taskflow::Logger::instance().init("tracker", config.log_dir);
    taskflow::Logger::instance().setLevel(config.log_level);
    
    LOG_INFO("Starting TaskFlow Tracker...");
    LOG_INFO("Config - Port: {}, Redis: {}", config.port, config.redis_addr);
    
    auto redis = std::make_shared<taskflow::RedisClient>("localhost", 6379);
    if (!redis->connect()) {
        LOG_ERROR("Failed to connect to Redis");
        return 1;
    }
    
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    
    std::string addr = "0.0.0.0:" + std::to_string(config.port);
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    
    auto tracker_service = std::make_shared<TrackerServiceImpl>(redis);
    builder.RegisterService(tracker_service.get());
    
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    
    LOG_INFO("Tracker listening on {}", addr);
    
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    LOG_INFO("Tracker shutting down...");
    
    g_running = false;
    server->Shutdown();
    
    LOG_INFO("Tracker stopped");
    return 0;
}
