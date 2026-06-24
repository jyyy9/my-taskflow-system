#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <atomic>
#include <csignal>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include "proto/taskflow.grpc.pb.h"
#include "common/logger.h"
#include "common/config_loader.h"
#include "common/redis_client.h"
#include "common/etcd_client.h"
#include "common/snowflake.h"
#include "scheduler/load_balancer.h"
#include "scheduler/task_dispatcher.h"

namespace {
    std::atomic<bool> g_running(true);
    std::shared_ptr<taskflow::RedisPool> g_redis_pool;
    std::shared_ptr<taskflow::LoadBalancer> g_load_balancer;
    std::shared_ptr<taskflow::TaskDispatcher> g_task_dispatcher;
}

void signalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        LOG_INFO("Received shutdown signal");
        g_running = false;
    }
}

class SchedulerServiceImpl final : public taskflow::SchedulerService::Service {
public:
    SchedulerServiceImpl(std::shared_ptr<taskflow::TaskDispatcher> dispatcher,
                         std::shared_ptr<taskflow::LoadBalancer> load_balancer)
        : dispatcher_(dispatcher), load_balancer_(load_balancer) {
    }
    
    grpc::Status SubmitTask(grpc::ServerContext* context,
                            const taskflow::SubmitTaskRequest* request,
                            taskflow::SubmitTaskReply* reply) override {
        std::string task_id = std::to_string(taskflow::Snowflake::instance().nextId());
        
        taskflow::TaskRequest task;
        task.id = task_id;
        task.type = request->type();
        task.priority = request->priority();
        task.data = request->data();
        task.created_at = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        
        if (dispatcher_->submitTask(task)) {
            reply->set_success(true);
            reply->set_task_id(task_id);
            reply->set_message("Task submitted successfully");
            LOG_INFO("Task {} submitted via gRPC", task_id);
        } else {
            reply->set_success(false);
            reply->set_message("Failed to submit task");
        }
        
        return grpc::Status::OK;
    }
    
    grpc::Status GetTaskStatus(grpc::ServerContext* context,
                              const taskflow::TaskStatusRequest* request,
                              taskflow::TaskStatusReply* reply) override {
        std::string task_id = request->task_id();
        
        taskflow::TaskInfo task_info = dispatcher_->getTaskInfo(task_id);
        
        if (task_info.id.empty()) {
            reply->set_success(false);
            reply->set_message("Task not found");
        } else {
            reply->set_success(true);
            reply->set_message("Task found");
            
            taskflow::Task* task = reply->mutable_task();
            task->set_id(task_info.id);
            task->set_type(static_cast<taskflow::TaskType>(task_info.type));
            task->set_priority(static_cast<taskflow::TaskPriority>(task_info.priority));
            task->set_data(task_info.data);
            task->set_created_at(task_info.created_at);
            task->set_started_at(task_info.started_at);
            task->set_finished_at(task_info.finished_at);
            task->set_status(static_cast<taskflow::TaskStatus>(task_info.status));
            task->set_retry_count(task_info.retry_count);
            task->set_error_message(task_info.error_message);
            task->set_result(task_info.result);
        }
        
        return grpc::Status::OK;
    }
    
    grpc::Status RegisterWorker(grpc::ServerContext* context,
                               const taskflow::RegisterWorkerRequest* request,
                               taskflow::RegisterWorkerReply* reply) override {
        std::string worker_id = request->worker_id();
        std::string address = request->address();
        int port = request->port();
        
        LOG_INFO("Worker registered: {} at {}:{}", worker_id, address, port);
        
        reply->set_success(true);
        reply->set_message("Worker registered successfully");
        
        return grpc::Status::OK;
    }
    
    grpc::Status GetWorkers(grpc::ServerContext* context,
                           const taskflow::GetWorkersRequest* request,
                           taskflow::GetWorkersReply* reply) override {
        std::vector<taskflow::WorkerNode> workers = load_balancer_->getWorkers();
        
        for (const auto& worker : workers) {
            taskflow::WorkerInfo* info = reply->add_workers();
            info->set_worker_id(worker.id);
            info->set_address(worker.address);
            info->set_port(worker.port);
            info->set_current_load(worker.current_load);
            info->set_status(worker.status);
            info->set_last_heartbeat(worker.last_heartbeat);
        }
        
        return grpc::Status::OK;
    }

private:
    std::shared_ptr<taskflow::TaskDispatcher> dispatcher_;
    std::shared_ptr<taskflow::LoadBalancer> load_balancer_;
};

class WorkerServiceImpl final : public taskflow::WorkerService::Service {
public:
    WorkerServiceImpl(std::shared_ptr<taskflow::TaskDispatcher> dispatcher,
                      std::shared_ptr<taskflow::LoadBalancer> load_balancer)
        : dispatcher_(dispatcher), load_balancer_(load_balancer) {
    }
    
    grpc::Status ExecuteTask(grpc::ServerContext* context,
                             const taskflow::ExecuteTaskRequest* request,
                             taskflow::ExecuteTaskReply* reply) override {
        reply->set_task_id(request->task_id());
        reply->set_success(true);
        reply->set_result("Task executed");
        return grpc::Status::OK;
    }
    
    grpc::Status Heartbeat(grpc::ServerContext* context,
                          const taskflow::HeartbeatRequest* request,
                          taskflow::HeartbeatReply* reply) override {
        std::string worker_id = request->worker_id();
        int current_load = request->current_load();
        
        load_balancer_->updateWorkerLoad(worker_id, current_load);
        
        reply->set_success(true);
        reply->set_message("Heartbeat received");
        
        return grpc::Status::OK;
    }

private:
    std::shared_ptr<taskflow::TaskDispatcher> dispatcher_;
    std::shared_ptr<taskflow::LoadBalancer> load_balancer_;
};

void workerCheckLoop(std::shared_ptr<taskflow::LoadBalancer> load_balancer,
                     std::shared_ptr<taskflow::TaskDispatcher> dispatcher,
                     int check_interval) {
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(check_interval));
        
        auto workers = load_balancer->getWorkers();
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        
        for (const auto& worker : workers) {
            if (now - worker.last_heartbeat > 15000) {
                LOG_WARN("Worker {} missed heartbeat, marking as dead", worker.id);
                load_balancer->markWorkerDead(worker.id);
            }
        }
    }
}

void taskDispatchLoop(std::shared_ptr<taskflow::TaskDispatcher> dispatcher,
                     std::shared_ptr<taskflow::LoadBalancer> load_balancer,
                     std::shared_ptr<taskflow::RedisPool> redis_pool,
                     int timeout_seconds) {
    while (g_running) {
        std::string task_id = dispatcher->popNextTask();
        
        if (task_id.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
        
        std::string worker_id = load_balancer->selectWorker(task_id);
        
        if (worker_id.empty()) {
            LOG_WARN("No available worker for task {}, re-queueing", task_id);
            
            auto redis = redis_pool->getConnection();
            if (redis) {
                taskflow::TaskInfo task = redis->getTask(task_id);
                if (!task.id.empty()) {
                    redis->pushToPriorityQueue(task.priority, task_id);
                }
                redis_pool->returnConnection(redis);
            }
            
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        
        dispatcher->assignTaskToWorker(task_id, worker_id);
        
        LOG_INFO("Task {} dispatched to worker {}", task_id, worker_id);
    }
}

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    std::string config_path = "config/scheduler.yaml";
    if (argc > 1) {
        config_path = argv[1];
    }
    
    taskflow::SchedulerConfig config = taskflow::loadSchedulerConfig(config_path);
    
    taskflow::Logger::instance().init("scheduler", config.log_dir);
    taskflow::Logger::instance().setLevel(config.log_level);
    
    LOG_INFO("Starting TaskFlow Scheduler...");
    LOG_INFO("Config - Port: {}, Load Balance: {}", config.port, config.load_balance);
    
    taskflow::Snowflake::instance().init(1);
    
    taskflow::ServiceRegistry::instance().init(config.etcd_endpoints);
    
    std::string addr = "0.0.0.0:" + std::to_string(config.port);
    
    if (!taskflow::ServiceRegistry::instance().registerService(
            "scheduler", "scheduler-1", "localhost", config.port, {})) {
        LOG_ERROR("Failed to register scheduler with etcd");
    } else {
        taskflow::ServiceRegistry::instance().startRefreshLoop("scheduler", "scheduler-1", 5);
    }
    
    std::vector<std::string> redis_parts;
    std::istringstream redis_stream(config.redis_addr);
    std::string redis_host;
    int redis_port = 6379;
    std::getline(redis_stream, redis_host, ':');
    if (!redis_stream.str().empty()) {
        redis_port = std::stoi(redis_stream.str());
    }
    
    g_redis_pool = std::make_shared<taskflow::RedisPool>(redis_host, redis_port, 4);
    
    g_load_balancer = std::make_shared<taskflow::LoadBalancer>(config.load_balance);
    
    g_task_dispatcher = std::make_shared<taskflow::TaskDispatcher>(g_redis_pool, g_load_balancer);
    
    std::vector<taskflow::ServiceInfo> initial_workers = 
        taskflow::ServiceRegistry::instance().getServices("worker");
    g_load_balancer->updateWorkers(initial_workers);
    
    std::thread worker_checker(workerCheckLoop, g_load_balancer, g_task_dispatcher, 
                               config.worker_check_interval);
    
    std::thread dispatcher_thread(taskDispatchLoop, g_task_dispatcher, g_load_balancer,
                                  g_redis_pool, config.task_timeout_seconds);
    
    grpc::EnableDefaultHealthCheckService(true);
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    
    auto scheduler_service = std::make_shared<SchedulerServiceImpl>(g_task_dispatcher, g_load_balancer);
    auto worker_service = std::make_shared<WorkerServiceImpl>(g_task_dispatcher, g_load_balancer);
    
    builder.RegisterService(scheduler_service.get());
    builder.RegisterService(worker_service.get());
    
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    
    LOG_INFO("Scheduler listening on {}", addr);
    
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        auto workers = taskflow::ServiceRegistry::instance().getServices("worker");
        g_load_balancer->updateWorkers(workers);
    }
    
    LOG_INFO("Scheduler shutting down...");
    
    taskflow::ServiceRegistry::instance().shutdown();
    
    g_running = false;
    
    if (worker_checker.joinable()) worker_checker.join();
    if (dispatcher_thread.joinable()) dispatcher_thread.join();
    
    server->Shutdown();
    
    LOG_INFO("Scheduler stopped");
    return 0;
}
