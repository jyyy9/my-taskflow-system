#pragma once

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <functional>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <curl/curl.h>

namespace taskflow {

struct ServiceInfo {
    std::string name;
    std::string id;
    std::string address;
    int port;
    std::map<std::string, std::string> metadata;
    int64_t lease_id;
    int64_t last_update;
};

class EtcdClient {
public:
    EtcdClient(const std::string& endpoints);
    ~EtcdClient();
    
    bool connect();
    void disconnect();
    bool isConnected() const { return connected_; }
    
    bool put(const std::string& key, const std::string& value, int64_t lease_id = 0);
    std::string get(const std::string& key);
    bool del(const std::string& key);
    
    std::vector<std::string> getPrefix(const std::string& prefix);
    std::map<std::string, std::string> getAllWithPrefix(const std::string& prefix);
    
    int64_t createLease(int ttl_seconds);
    bool keepAlive(int64_t lease_id);
    bool revokeLease(int64_t lease_id);
    
    int64_t registerService(const std::string& name, const std::string& id, 
                            const std::string& address, int port,
                            const std::map<std::string, std::string>& metadata = {},
                            int ttl_seconds = 10);
    
    bool deregisterService(const std::string& name, const std::string& id);
    
    std::vector<ServiceInfo> getServices(const std::string& name);
    ServiceInfo getService(const std::string& name, const std::string& id);
    
    bool refreshService(const std::string& name, const std::string& id);
    
private:
    std::string endpoints_;
    bool connected_;
    std::mutex curl_mutex_;
    
    std::string encodeValue(const std::string& value);
    std::string decodeValue(const std::string& value);
    
    std::string makeRequest(const std::string& method, const std::string& path, 
                           const std::string& body = "", bool* success = nullptr);
};

class ServiceRegistry {
public:
    static ServiceRegistry& instance();
    
    void init(const std::string& etcd_endpoints);
    
    bool registerService(const std::string& name, const std::string& id,
                       const std::string& address, int port,
                       const std::map<std::string, std::string>& metadata = {});
    
    bool deregisterService(const std::string& name, const std::string& id);
    
    bool startRefreshLoop(const std::string& name, const std::string& id, int interval_seconds = 5);
    void stopRefreshLoop();
    
    std::vector<ServiceInfo> getServices(const std::string& name);
    ServiceInfo getService(const std::string& name, const std::string& id);
    
    void shutdown();

private:
    ServiceRegistry() : running_(false), refresh_thread_(nullptr) {}
    ~ServiceRegistry() { shutdown(); }
    ServiceRegistry(const ServiceRegistry&) = delete;
    ServiceRegistry& operator=(const ServiceRegistry&) = delete;
    
    std::unique_ptr<EtcdClient> client_;
    std::string service_name_;
    std::string service_id_;
    std::atomic<bool> running_;
    std::thread* refresh_thread_;
    std::mutex mutex_;
};

}
