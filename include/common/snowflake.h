#pragma once

#include <cstdint>
#include <mutex>

namespace taskflow {

class Snowflake {
public:
    static Snowflake& instance();
    
    void init(int64_t worker_id);
    int64_t nextId();
    
    int64_t getWorkerId() const { return worker_id_; }

private:
    Snowflake() : worker_id_(0), datacenter_id_(0), sequence_(0), last_timestamp_(-1) {}
    ~Snowflake() = default;
    Snowflake(const Snowflake&) = delete;
    Snowflake& operator=(const Snowflake&) = delete;
    
    int64_t nextIdLocked();
    int64_t waitNextMillis(int64_t timestamp);
    
    static constexpr int64_t kTwepoch = 1609459200000LL;
    static constexpr int64_t kWorkerIdBits = 5LL;
    static constexpr int64_t kDatacenterIdBits = 5LL;
    static constexpr int64_t kSequenceBits = 12LL;
    
    static constexpr int64_t kMaxWorkerId = (1LL << kWorkerIdBits) - 1;
    static constexpr int64_t kMaxDatacenterId = (1LL << kDatacenterIdBits) - 1;
    
    static constexpr int64_t kWorkerIdShift = kSequenceBits;
    static constexpr int64_t kDatacenterIdShift = kSequenceBits + kWorkerIdBits;
    static constexpr int64_t kTimestampLeftShift = kSequenceBits + kWorkerIdBits + kDatacenterIdBits;
    static constexpr int64_t kSequenceMask = (1LL << kSequenceBits) - 1;
    
    int64_t worker_id_;
    int64_t datacenter_id_;
    int64_t sequence_;
    int64_t last_timestamp_;
    std::mutex mutex_;
};

}
