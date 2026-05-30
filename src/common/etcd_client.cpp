#include "common/etcd_client.h"
#include "common/logger.h"
#include <sstream>
#include <iomanip>
#include <regex>
#include <chrono>

namespace taskflow {

static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

EtcdClient::EtcdClient(const std::string& endpoints) 
    : endpoints_(endpoints), connected_(false) {
}

EtcdClient::~EtcdClient() {
    disconnect();
}

bool EtcdClient::connect() {
    std::lock_guard<std::mutex> lock(curl_mutex_);
    
    CURL* curl = curl_easy_init();
    if (!curl) {
        return false;
    }
    
    std::string endpoint = endpoints_;
    if (endpoint.find("http://") != 0 && endpoint.find("https://") != 0) {
        endpoint = "http://" + endpoint;
    }
    
    std::string health_url = endpoint + "/health";
    curl_easy_setopt(curl, CURLOPT_URL, health_url.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 2L);
    
    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    curl_easy_cleanup(curl);
    
    connected_ = (res == CURLE_OK && http_code >= 200 && http_code < 400);
    
    if (connected_) {
        LOG_INFO("Connected to etcd at {}", endpoints_);
    } else {
        LOG_ERROR("Failed to connect to etcd at {}: curl error {}, http code {}", 
                  endpoints_, res, http_code);
    }
    
    return connected_;
}

void EtcdClient::disconnect() {
    connected_ = false;
}

std::string EtcdClient::encodeValue(const std::string& value) {
    std::ostringstream oss;
    for (unsigned char c : value) {
        if (c == '"' || c == '\\' || c < 0x20) {
            oss << "\\u" << std::hex << std::setw(2) << std::setfill('0') << (int)c;
        } else {
            oss << c;
        }
    }
    return oss.str();
}

std::string EtcdClient::decodeValue(const std::string& value) {
    std::string result;
    for (size_t i = 0; i < value.length(); i++) {
        if (value[i] == '\\' && i + 1 < value.length() && value[i + 1] == 'u' && 
            i + 5 < value.length()) {
            std::string hex = value.substr(i + 2, 4);
            char c = (char)std::stoi(hex, nullptr, 16);
            result += c;
            i += 5;
        } else {
            result += value[i];
        }
    }
    return result;
}

std::string EtcdClient::makeRequest(const std::string& method, const std::string& path,
                                   const std::string& body, bool* success) {
    std::lock_guard<std::mutex> lock(curl_mutex_);
    
    CURL* curl = curl_easy_init();
    if (!curl) {
        if (success) *success = false;
        return "";
    }
    
    std::string endpoint = endpoints_;
    if (endpoint.find("http://") != 0 && endpoint.find("https://") != 0) {
        endpoint = "http://" + endpoint;
    }
    
    std::string url = endpoint + path;
    std::string response;
    
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
    
    if (!body.empty()) {
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    }
    
    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    
    curl_easy_cleanup(curl);
    
    if (success) {
        *success = (res == CURLE_OK && http_code >= 200 && http_code < 300);
    }
    
    if (res != CURLE_OK) {
        LOG_ERROR("etcd request failed: {} to {}", curl_easy_strerror(res), url);
    }
    
    return response;
}

bool EtcdClient::put(const std::string& key, const std::string& value, int64_t lease_id) {
    std::ostringstream body;
    body << "{\"key\":\"" << encodeValue(key) << "\",\"value\":\"" << encodeValue(value) << "\"";
    if (lease_id > 0) {
        body << ",\"lease\":" << lease_id;
    }
    body << "}";
    
    bool success;
    std::string response = makeRequest("PUT", "/v3/kv/put", body.str(), &success);
    
    if (success) {
        LOG_DEBUG("etcd put: {} = {}", key, value);
    }
    
    return success;
}

std::string EtcdClient::get(const std::string& key) {
    std::ostringstream body;
    body << "{\"key\":\"" << encodeValue(key) << "\"}";
    
    bool success;
    std::string response = makeRequest("POST", "/v3/kv/range", body.str(), &success);
    
    if (!success) return "";
    
    std::regex value_regex("\"value\":\"([^\"]*)\"");
    std::smatch match;
    if (std::regex_search(response, match, value_regex)) {
        return decodeValue(match[1].str());
    }
    
    return "";
}

bool EtcdClient::del(const std::string& key) {
    std::ostringstream body;
    body << "{\"key\":\"" << encodeValue(key) << "\"}";
    
    bool success;
    std::string response = makeRequest("DELETE", "/v3/kv/del", body.str(), &success);
    
    return success;
}

std::vector<std::string> EtcdClient::getPrefix(const std::string& prefix) {
    std::vector<std::string> result;
    
    std::ostringstream body;
    body << "{\"key\":\"" << encodeValue(prefix) << "\",\"range_end\":\"" 
         << encodeValue(prefix) << "\",\"count_only\":false}";
    
    bool success;
    std::string response = makeRequest("POST", "/v3/kv/range", body.str(), &success);
    
    if (!success) return result;
    
    std::regex key_regex("\"key\":\"([^\"]*)\"");
    std::smatch match;
    std::string::const_iterator search_start(response.cbegin());
    while (std::regex_search(search_start, response.cend(), match, key_regex)) {
        result.push_back(decodeValue(match[1].str()));
        search_start = match.suffix().first;
    }
    
    return result;
}

std::map<std::string, std::string> EtcdClient::getAllWithPrefix(const std::string& prefix) {
    std::map<std::string, std::string> result;
    
    std::vector<std::string> keys = getPrefix(prefix);
    for (const auto& key : keys) {
        std::string value = get(key);
        if (!value.empty()) {
            result[key] = value;
        }
    }
    
    return result;
}

int64_t EtcdClient::createLease(int ttl_seconds) {
    std::ostringstream body;
    body << "{\"TTL\":" << ttl_seconds << "}";
    
    bool success;
    std::string response = makeRequest("POST", "/v3/lease/grant", body.str(), &success);
    
    if (!success) return 0;
    
    std::regex id_regex("\"ID\":([0-9]+)");
    std::smatch match;
    if (std::regex_search(response, match, id_regex)) {
        return std::stoll(match[1].str());
    }
    
    return 0;
}

bool EtcdClient::keepAlive(int64_t lease_id) {
    std::ostringstream body;
    body << "{\"ID\":" << lease_id << "}";
    
    bool success;
    std::string response = makeRequest("POST", "/v3/lease/keepalive", body.str(), &success);
    
    return success;
}

bool EtcdClient::revokeLease(int64_t lease_id) {
    std::ostringstream body;
    body << "{\"ID\":" << lease_id << "}";
    
    bool success;
    std::string response = makeRequest("POST", "/v3/lease/revoke", body.str(), &success);
    
    return success;
}

int64_t EtcdClient::registerService(const std::string& name, const std::string& id,
                                   const std::string& address, int port,
                                   const std::map<std::string, std::string>& metadata,
                                   int ttl_seconds) {
    int64_t lease_id = createLease(ttl_seconds);
    if (lease_id == 0) {
        LOG_ERROR("Failed to create lease for service {}", name);
        return 0;
    }
    
    std::string key = "/services/" + name + "/" + id;
    std::ostringstream value;
    value << "{\"address\":\"" << address << "\",\"port\":" << port;
    
    if (!metadata.empty()) {
        value << ",\"metadata\":{";
        bool first = true;
        for (const auto& m : metadata) {
            if (!first) value << ",";
            value << "\"" << m.first << "\":\"" << m.second << "\"";
            first = false;
        }
        value << "}";
    }
    
    value << "}";
    
    if (put(key, value.str(), lease_id)) {
        LOG_INFO("Registered service {}:{} at {}:{}", name, id, address, port);
        return lease_id;
    }
    
    revokeLease(lease_id);
    return 0;
}

bool EtcdClient::deregisterService(const std::string& name, const std::string& id) {
    std::string key = "/services/" + name + "/" + id;
    bool success = del(key);
    
    if (success) {
        LOG_INFO("Deregistered service {}:{}", name, id);
    }
    
    return success;
}

std::vector<ServiceInfo> EtcdClient::getServices(const std::string& name) {
    std::vector<ServiceInfo> result;
    
    std::string prefix = "/services/" + name + "/";
    auto services = getAllWithPrefix(prefix);
    
    for (const auto& pair : services) {
        std::string key = pair.first;
        std::string value = pair.second;
        
        ServiceInfo info;
        info.name = name;
        
        size_t last_slash = key.find_last_of('/');
        if (last_slash != std::string::npos) {
            info.id = key.substr(last_slash + 1);
        }
        
        std::regex addr_regex("\"address\":\"([^\"]*)\"");
        std::regex port_regex("\"port\":([0-9]+)");
        std::regex meta_regex("\"metadata\":\\{([^}]*)\\}");
        
        std::smatch match;
        if (std::regex_search(value, match, addr_regex)) {
            info.address = match[1].str();
        }
        if (std::regex_search(value, match, port_regex)) {
            info.port = std::stoi(match[1].str());
        }
        
        result.push_back(info);
    }
    
    return result;
}

ServiceInfo EtcdClient::getService(const std::string& name, const std::string& id) {
    std::string key = "/services/" + name + "/" + id;
    std::string value = get(key);
    
    ServiceInfo info;
    info.name = name;
    info.id = id;
    
    if (value.empty()) {
        return info;
    }
    
    std::regex addr_regex("\"address\":\"([^\"]*)\"");
    std::regex port_regex("\"port\":([0-9]+)");
    
    std::smatch match;
    if (std::regex_search(value, match, addr_regex)) {
        info.address = match[1].str();
    }
    if (std::regex_search(value, match, port_regex)) {
        info.port = std::stoi(match[1].str());
    }
    
    return info;
}

bool EtcdClient::refreshService(const std::string& name, const std::string& id) {
    std::string key = "/services/" + name + "/" + id;
    std::string value = get(key);
    
    if (value.empty()) {
        return false;
    }
    
    std::regex addr_regex("\"address\":\"([^\"]*)\"");
    std::regex port_regex("\"port\":([0-9]+)");
    
    std::smatch match;
    std::string address;
    int port = 0;
    
    if (std::regex_search(value, match, addr_regex)) {
        address = match[1].str();
    }
    if (std::regex_search(value, match, port_regex)) {
        port = std::stoi(match[1].str());
    }
    
    int64_t lease_id = createLease(10);
    if (lease_id == 0) {
        return false;
    }
    
    return put(key, value, lease_id);
}

ServiceRegistry& ServiceRegistry::instance() {
    static ServiceRegistry instance;
    return instance;
}

void ServiceRegistry::init(const std::string& etcd_endpoints) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (client_) {
        client_.reset();
    }
    
    client_ = std::make_unique<EtcdClient>(etcd_endpoints);
    client_->connect();
    
    LOG_INFO("ServiceRegistry initialized with etcd: {}", etcd_endpoints);
}

bool ServiceRegistry::registerService(const std::string& name, const std::string& id,
                                      const std::string& address, int port,
                                      const std::map<std::string, std::string>& metadata) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!client_) {
        LOG_ERROR("ServiceRegistry not initialized");
        return false;
    }
    
    int64_t lease_id = client_->registerService(name, id, address, port, metadata, 10);
    
    if (lease_id > 0) {
        service_name_ = name;
        service_id_ = id;
        return true;
    }
    
    return false;
}

bool ServiceRegistry::deregisterService(const std::string& name, const std::string& id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!client_) {
        LOG_ERROR("ServiceRegistry not initialized");
        return false;
    }
    
    return client_->deregisterService(name, id);
}

bool ServiceRegistry::startRefreshLoop(const std::string& name, const std::string& id, 
                                       int interval_seconds) {
    running_ = true;
    service_name_ = name;
    service_id_ = id;
    
    refresh_thread_ = new std::thread([this, name, id, interval_seconds]() {
        while (running_) {
            {
                std::this_thread::sleep_for(std::chrono::seconds(interval_seconds));
                std::lock_guard<std::mutex> lock(mutex_);
                
                if (!running_) break;
                
                if (client_ && !service_name_.empty() && !service_id_.empty()) {
                    client_->refreshService(service_name_, service_id_);
                }
            }
        }
    });
    
    return true;
}

void ServiceRegistry::stopRefreshLoop() {
    running_ = false;
    
    if (refresh_thread_ && refresh_thread_->joinable()) {
        refresh_thread_->join();
        delete refresh_thread_;
        refresh_thread_ = nullptr;
    }
}

std::vector<ServiceInfo> ServiceRegistry::getServices(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!client_) {
        return {};
    }
    
    return client_->getServices(name);
}

ServiceInfo ServiceRegistry::getService(const std::string& name, const std::string& id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!client_) {
        return {};
    }
    
    return client_->getService(name, id);
}

void ServiceRegistry::shutdown() {
    stopRefreshLoop();
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!service_name_.empty() && !service_id_.empty()) {
        if (client_) {
            client_->deregisterService(service_name_, service_id_);
        }
    }
    
    client_.reset();
    LOG_INFO("ServiceRegistry shutdown");
}

}
