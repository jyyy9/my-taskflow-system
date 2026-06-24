#pragma once

#include <string>
#include <memory>
#include <functional>
#include <map>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "proto/taskflow.grpc.pb.h"

namespace taskflow {

using HttpCallback = std::function<void(const std::string&, int)>;

struct HttpRequest {
    std::string method;
    std::string path;
    std::string body;
    std::map<std::string, std::string> headers;
    std::map<std::string, std::string> query_params;
};

struct HttpResponse {
    int status_code;
    std::string body;
    std::map<std::string, std::string> headers;
};

class HttpHandler {
public:
    HttpHandler(const std::string& scheduler_addr);
    
    bool init();
    void shutdown();
    
    void handleRequest(const HttpRequest& request, HttpResponse& response);
    
    void setSubmitTaskCallback(std::function<bool(const HttpRequest&, HttpResponse&)> callback);
    void setGetStatusCallback(std::function<bool(const HttpRequest&, HttpResponse&)> callback);

private:
    bool handleSubmitTask(const HttpRequest& request, HttpResponse& response);
    bool handleGetStatus(const HttpRequest& request, HttpResponse& response);
    bool handleGetWorkers(const HttpRequest& request, HttpResponse& response);
    bool handleGetStats(const HttpRequest& request, HttpResponse& response);
    bool handleGetQueueStatus(const HttpRequest& request, HttpResponse& response);
    bool handleHealthCheck(const HttpRequest& request, HttpResponse& response);
    
    std::string parseQueryParam(const std::string& query, const std::string& key);
    std::string extractPath(const std::string& path);
    std::map<std::string, std::string> parseQueryParams(const std::string& query);

private:
    std::string scheduler_addr_;
    std::unique_ptr<SchedulerService::Stub> scheduler_stub_;
    
    std::function<bool(const HttpRequest&, HttpResponse&)> submit_task_callback_;
    std::function<bool(const HttpRequest&, HttpResponse&)> get_status_callback_;
    
    std::mutex mutex_;
};

}
