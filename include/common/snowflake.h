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
    Snowflake() = default;
    ~Snowflake() = default;
    Snowflake(const Snowflake&) = delete;
    Snowflake& operator=(const Snowflake&) = delete;
    
    int64_t nextIdLocked();
    
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
    
    int64_t worker_id_ = 0;
    int64_t datacenter_id_ = 0;
    int64_t sequence_ = 0;
    int64_t last_timestamp_ = -1;
    std::mutex mutex_;
};

}
