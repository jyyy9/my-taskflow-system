#include "common/logger.h"
#include <filesystem>

namespace taskflow {

Logger& Logger::instance() {
    static Logger instance;
    return instance;
}

void Logger::init(const std::string& service_name, const std::string& log_dir) {
    try {
        std::filesystem::create_directories(log_dir);
        
        auto rotating_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            log_dir + "/" + service_name + ".log",
            100 * 1024 * 1024,
            10
        );
        
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        
        console_sink->set_level(spdlog::level::debug);
        rotating_sink->set_level(spdlog::level::debug);
        
        logger_ = std::make_shared<spdlog::logger>(
            service_name,
            spdlog::sinks_init_list({console_sink, rotating_sink})
        );
        
        logger_->set_level(spdlog::level::debug);
        logger_->flush_on(spdlog::level::info);
        
        spdlog::register_logger(logger_);
        
        LOG_INFO("Logger initialized for service: {}", service_name);
    } catch (const spdlog::spdlog_ex& ex) {
        std::cerr << "Log initialization failed: " << ex.what() << std::endl;
    }
}

void Logger::setLevel(const std::string& level) {
    if (!logger_) return;
    
    if (level == "debug") {
        logger_->set_level(spdlog::level::debug);
    } else if (level == "info") {
        logger_->set_level(spdlog::level::info);
    } else if (level == "warn") {
        logger_->set_level(spdlog::level::warn);
    } else if (level == "error") {
        logger_->set_level(spdlog::level::err);
    }
}

}
