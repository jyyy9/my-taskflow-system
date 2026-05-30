#pragma once

#include <string>
#include <memory>
#include <chrono>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace taskflow {

class Logger {
public:
    static Logger& instance();
    
    void init(const std::string& service_name, const std::string& log_dir = "./logs");
    
    std::shared_ptr<spdlog::logger> getLogger() { return logger_; }
    
    template<typename... Args>
    void debug(const char* fmt, Args&&... args) {
        if (logger_) {
            logger_->debug(fmt, std::forward<Args>(args)...);
        }
    }
    
    template<typename... Args>
    void info(const char* fmt, Args&&... args) {
        if (logger_) {
            logger_->info(fmt, std::forward<Args>(args)...);
        }
    }
    
    template<typename... Args>
    void warn(const char* fmt, Args&&... args) {
        if (logger_) {
            logger_->warn(fmt, std::forward<Args>(args)...);
        }
    }
    
    template<typename... Args>
    void error(const char* fmt, Args&&... args) {
        if (logger_) {
            logger_->error(fmt, std::forward<Args>(args)...);
        }
    }
    
    void setLevel(const std::string& level);

private:
    Logger() = default;
    ~Logger() = default;
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    
    std::shared_ptr<spdlog::logger> logger_;
};

#define LOG_DEBUG(...) taskflow::Logger::instance().debug(__VA_ARGS__)
#define LOG_INFO(...) taskflow::Logger::instance().info(__VA_ARGS__)
#define LOG_WARN(...) taskflow::Logger::instance().warn(__VA_ARGS__)
#define LOG_ERROR(...) taskflow::Logger::instance().error(__VA_ARGS__)

}
