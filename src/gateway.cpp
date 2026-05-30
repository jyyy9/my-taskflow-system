#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <csignal>
#include <boost/asio.hpp>
#include "common/logger.h"
#include "common/config_loader.h"
#include "gateway/http_handler.h"

namespace {
    std::atomic<bool> g_running(true);
}

void signalHandler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        LOG_INFO("Received signal {}, shutting down...", signal);
        g_running = false;
    }
}

class SimpleHttpServer {
public:
    SimpleHttpServer(int port, std::shared_ptr<taskflow::HttpHandler> handler)
        : port_(port), handler_(handler), acceptor_(io_context_) {
    }
    
    bool start() {
        boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::tcp::v4(), port_);
        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(boost::asio::ip::tcp::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen();
        
        LOG_INFO("HTTP server listening on port {}", port_);
        
        startAccept();
        
        return true;
    }
    
    void run() {
        io_context_.run();
    }
    
    void stop() {
        io_context_.stop();
    }

private:
    void startAccept() {
        auto socket = std::make_shared<boost::asio::ip::tcp::socket>(io_context_);
        acceptor_.async_accept(*socket, [this, socket](const boost::system::error_code& ec) {
            if (!ec) {
                std::thread([this, socket]() {
                    handleConnection(socket);
                }).detach();
            }
            startAccept();
        });
    }
    
    void handleConnection(std::shared_ptr<boost::asio::ip::tcp::socket> socket) {
        try {
            boost::asio::streambuf buffer;
            boost::asio::read_until(*socket, buffer, "\r\n\r\n");
            
            std::istream stream(&buffer);
            std::string request_line;
            std::getline(stream, request_line);
            
            std::string method, path, version;
            std::istringstream parser(request_line);
            parser >> method >> path >> version;
            
            taskflow::HttpRequest request;
            request.method = method;
            request.path = path;
            
            size_t body_size = 0;
            std::string line;
            while (std::getline(stream, line) && line != "\r") {
                if (line.find("Content-Length:") != std::string::npos) {
                    size_t colon = line.find(':');
                    if (colon != std::string::npos) {
                        body_size = std::stoi(line.substr(colon + 1));
                    }
                }
            }
            
            if (body_size > 0) {
                if (buffer.size() < body_size) {
                    boost::asio::read(*socket, buffer, boost::asio::transfer_exactly(body_size - buffer.size()));
                }
                std::string body(static_cast<const char*>(buffer.data().data()) + 
                                 (buffer.size() - body_size), body_size);
                request.body = body;
            }
            
            taskflow::HttpResponse response;
            handler_->handleRequest(request, response);
            
            std::ostringstream response_stream;
            response_stream << "HTTP/1.1 " << response.status_code << " OK\r\n";
            response_stream << "Content-Type: " << response.headers["Content-Type"] << "\r\n";
            response_stream << "Content-Length: " << response.body.size() << "\r\n";
            response_stream << "Connection: close\r\n";
            response_stream << "\r\n";
            response_stream << response.body;
            
            boost::asio::write(*socket, boost::asio::buffer(response_stream.str()));
            
        } catch (const std::exception& e) {
            LOG_ERROR("Error handling connection: {}", e.what());
        }
        
        try {
            socket->close();
        } catch (...) {}
    }
    
    int port_;
    std::shared_ptr<taskflow::HttpHandler> handler_;
    boost::asio::io_context io_context_;
    boost::asio::ip::tcp::acceptor acceptor_;
};

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    std::string config_path = "config/gateway.yaml";
    if (argc > 1) {
        config_path = argv[1];
    }
    
    taskflow::GatewayConfig config = taskflow::loadGatewayConfig(config_path);
    
    taskflow::Logger::instance().init("gateway", config.log_dir);
    taskflow::Logger::instance().setLevel(config.log_level);
    
    LOG_INFO("Starting TaskFlow Gateway...");
    LOG_INFO("Config - Port: {}, Scheduler: {}", config.port, config.scheduler_addr);
    
    auto http_handler = std::make_shared<taskflow::HttpHandler>(config.scheduler_addr);
    if (!http_handler->init()) {
        LOG_ERROR("Failed to initialize HTTP handler");
        return 1;
    }
    
    SimpleHttpServer server(config.port, http_handler);
    if (!server.start()) {
        LOG_ERROR("Failed to start HTTP server");
        return 1;
    }
    
    std::thread server_thread([&server]() {
        server.run();
    });
    
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    LOG_INFO("Gateway shutting down...");
    http_handler->shutdown();
    server.stop();
    
    if (server_thread.joinable()) {
        server_thread.join();
    }
    
    LOG_INFO("Gateway stopped");
    return 0;
}
