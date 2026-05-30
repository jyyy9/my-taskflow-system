#include "common/snowflake.h"
#include "common/logger.h"
#include <stdexcept>

namespace taskflow {

Snowflake& Snowflake::instance() {
    static Snowflake instance;
    return instance;
}

void Snowflake::init(int64_t worker_id) {
    if (worker_id < 0 || worker_id > kMaxWorkerId) {
        throw std::invalid_argument("Worker ID must be between 0 and " + std::to_string(kMaxWorkerId));
    }
    worker_id_ = worker_id;
    LOG_INFO("Snowflake initialized with worker_id: {}", worker_id);
}

int64_t Snowflake::nextId() {
    std::lock_guard<std::mutex> lock(mutex_);
    return nextIdLocked();
}

int64_t Snowflake::nextIdLocked() {
    int64_t timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now()
    ).time_since_epoch().count();
    
    if (timestamp < last_timestamp_) {
        LOG_ERROR("Clock moved backwards. Waiting for {}", last_timestamp_ - timestamp);
        throw std::runtime_error("Clock moved backwards");
    }
    
    if (last_timestamp_ == timestamp) {
        sequence_ = (sequence_ + 1) & kSequenceMask;
        if (sequence_ == 0) {
            timestamp = waitNextMillis(timestamp);
        }
    } else {
        sequence_ = 0;
    }
    
    last_timestamp_ = timestamp;
    
    return ((timestamp - kTwepoch) << kTimestampLeftShift)
        | (datacenter_id_ << kDatacenterIdShift)
        | (worker_id_ << kWorkerIdShift)
        | sequence_;
}

int64_t Snowflake::waitNextMillis(int64_t timestamp) {
    while (timestamp <= last_timestamp_) {
        timestamp = std::chrono::time_point_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now()
        ).time_since_epoch().count();
    }
    return timestamp;
}

}
