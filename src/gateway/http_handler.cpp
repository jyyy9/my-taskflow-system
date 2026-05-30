#include "gateway/http_handler.h"
#include "common/logger.h"
#include <sstream>
#include <regex>
#include <chrono>

namespace taskflow {

HttpHandler::HttpHandler(const std::string& scheduler_addr)
    : scheduler_addr_(scheduler_addr) {
}

bool HttpHandler::init() {
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(1024 * 1024 * 10);
    
    auto channel = grpc::CreateCustomChannel(
        scheduler_addr_,
        grpc::InsecureChannelCredentials(),
        args
    );
    
    scheduler_stub_ = SchedulerService::NewStub(channel);
    
    LOG_INFO("HTTP Handler initialized with scheduler at {}", scheduler_addr_);
    return true;
}

void HttpHandler::shutdown() {
    LOG_INFO("HTTP Handler shutting down");
}

void HttpHandler::setSubmitTaskCallback(
    std::function<bool(const HttpRequest&, HttpResponse&)> callback) {
    submit_task_callback_ = callback;
}

void HttpHandler::setGetStatusCallback(
    std::function<bool(const HttpRequest&, HttpResponse&)> callback) {
    get_status_callback_ = callback;
}

void HttpHandler::handleRequest(const HttpRequest& request, HttpResponse& response) {
    response.status_code = 404;
    response.body = "Not Found";
    response.headers["Content-Type"] = "application/json";
    
    std::string path = extractPath(request.path);
    
    LOG_DEBUG("HTTP {} {}", request.method, path);
    
    if (request.method == "GET" && path == "/health") {
        handleHealthCheck(request, response);
    } else if (request.method == "POST" && path == "/task") {
        handleSubmitTask(request, response);
    } else if (request.method == "GET" && path == "/task") {
        handleGetStatus(request, response);
    } else if (request.method == "GET" && path == "/workers") {
        handleGetWorkers(request, response);
    } else if (request.method == "GET" && path == "/stats") {
        handleGetStats(request, response);
    } else if (request.method == "GET" && path == "/queue") {
        handleGetQueueStatus(request, response);
    }
}

bool HttpHandler::handleSubmitTask(const HttpRequest& request, HttpResponse& response) {
    grpc::ClientContext context;
    
    SubmitTaskRequest grpc_request;
    SubmitTaskReply grpc_reply;
    
    std::regex type_regex("\"type\"\\s*:\\s*\"([^\"]+)\"");
    std::regex priority_regex("\"priority\"\\s*:\\s*(\\d+)");
    std::regex data_regex("\"data\"\\s*:\\s*\"([^\"]+)\"");
    
    std::smatch match;
    std::string body = request.body;
    
    if (std::regex_search(body, match, type_regex)) {
        std::string type = match[1].str();
        if (type == "video") {
            grpc_request.set_type(TaskType::VIDEO);
        } else if (type == "image") {
            grpc_request.set_type(TaskType::IMAGE);
        } else if (type == "data") {
            grpc_request.set_type(TaskType::DATA_EXPORT);
        }
    }
    
    if (std::regex_search(body, match, priority_regex)) {
        int priority = std::stoi(match[1].str());
        grpc_request.set_priority(static_cast<TaskPriority>(priority));
    } else {
        grpc_request.set_priority(TaskPriority::MEDIUM);
    }
    
    if (std::regex_search(body, match, data_regex)) {
        grpc_request.set_data(match[1].str());
    }
    
    grpc_request.set_data(body);
    
    grpc::Status status = scheduler_stub_->SubmitTask(&context, grpc_request, &grpc_reply);
    
    if (status.ok()) {
        response.status_code = grpc_reply.success() ? 200 : 500;
        std::ostringstream oss;
        oss << "{\"success\":" << (grpc_reply.success() ? "true" : "false")
            << ",\"task_id\":\"" << grpc_reply.task_id() << "\""
            << ",\"message\":\"" << grpc_reply.message() << "\"}";
        response.body = oss.str();
    } else {
        response.status_code = 503;
        std::ostringstream oss;
        oss << "{\"success\":false,\"error\":\"" << status.error_message() << "\"}";
        response.body = oss.str();
    }
    
    return status.ok();
}

bool HttpHandler::handleGetStatus(const HttpRequest& request, HttpResponse& response) {
    std::string task_id = parseQueryParam(request.path, "id");
    
    if (task_id.empty()) {
        response.status_code = 400;
        response.body = "{\"error\":\"Missing task id\"}";
        return false;
    }
    
    grpc::ClientContext context;
    TaskStatusRequest grpc_request;
    TaskStatusReply grpc_reply;
    
    grpc_request.set_task_id(task_id);
    
    grpc::Status status = scheduler_stub_->GetTaskStatus(&context, grpc_request, &grpc_reply);
    
    if (status.ok()) {
        response.status_code = grpc_reply.success() ? 200 : 404;
        
        std::ostringstream oss;
        oss << "{\"success\":" << (grpc_reply.success() ? "true" : "false")
            << ",\"task\":{"
            << "\"id\":\"" << grpc_reply.task().id() << "\""
            << ",\"status\":\"" << TaskStatus_Name(grpc_reply.task().status()) << "\""
            << ",\"created_at\":" << grpc_reply.task().created_at()
            << ",\"started_at\":" << grpc_reply.task().started_at()
            << ",\"finished_at\":" << grpc_reply.task().finished_at()
            << ",\"error_message\":\"" << grpc_reply.task().error_message() << "\""
            << ",\"result\":\"" << grpc_reply.task().result() << "\""
            << "}}";
        response.body = oss.str();
    } else {
        response.status_code = 503;
        response.body = "{\"error\":\"" + status.error_message() + "\"}";
    }
    
    return status.ok();
}

bool HttpHandler::handleGetWorkers(const HttpRequest& request, HttpResponse& response) {
    grpc::ClientContext context;
    GetWorkersRequest grpc_request;
    GetWorkersReply grpc_reply;
    
    grpc::Status status = scheduler_stub_->GetWorkers(&context, grpc_request, &grpc_reply);
    
    if (status.ok()) {
        response.status_code = 200;
        
        std::ostringstream oss;
        oss << "{\"workers\":[";
        
        for (int i = 0; i < grpc_reply.workers_size(); i++) {
            if (i > 0) oss << ",";
            const auto& worker = grpc_reply.workers(i);
            oss << "{"
                << "\"id\":\"" << worker.worker_id() << "\""
                << ",\"address\":\"" << worker.address() << "\""
                << ",\"port\":" << worker.port()
                << ",\"current_load\":" << worker.current_load()
                << ",\"status\":\"" << worker.status() << "\""
                << "}";
        }
        
        oss << "]}";
        response.body = oss.str();
    } else {
        response.status_code = 503;
        response.body = "{\"error\":\"" + status.error_message() + "\"}";
    }
    
    return status.ok();
}

bool HttpHandler::handleGetStats(const HttpRequest& request, HttpResponse& response) {
    response.status_code = 200;
    response.body = "{\"stats\":\"not implemented\"}";
    return true;
}

bool HttpHandler::handleHealthCheck(const HttpRequest& request, HttpResponse& response) {
    response.status_code = 200;
    response.body = "{\"status\":\"ok\",\"service\":\"gateway\"}";
    return true;
}

bool HttpHandler::handleGetQueueStatus(const HttpRequest& request, HttpResponse& response) {
    response.status_code = 200;
    response.body = "{\"queue\":\"not implemented\"}";
    return true;
}

std::string HttpHandler::parseQueryParam(const std::string& query, const std::string& key) {
    auto params = parseQueryParams(query);
    auto it = params.find(key);
    if (it != params.end()) {
        return it->second;
    }
    return "";
}

std::string HttpHandler::extractPath(const std::string& path) {
    size_t pos = path.find('?');
    if (pos != std::string::npos) {
        return path.substr(0, pos);
    }
    return path;
}

std::map<std::string, std::string> HttpHandler::parseQueryParams(const std::string& query) {
    std::map<std::string, std::string> params;
    
    size_t pos = query.find('?');
    std::string query_string = (pos != std::string::npos) ? query.substr(pos + 1) : query;
    
    std::istringstream stream(query_string);
    std::string pair;
    
    while (std::getline(stream, pair, '&')) {
        size_t eq_pos = pair.find('=');
        if (eq_pos != std::string::npos) {
            std::string key = pair.substr(0, eq_pos);
            std::string value = pair.substr(eq_pos + 1);
            params[key] = value;
        }
    }
    
    return params;
}

}
