# TaskFlow - 分布式任务调度系统

一个基于C++实现的分布式任务调度系统，采用gRPC & Protobuf进行服务间通信，微服务架构。

## 系统架构

​         ![Uploading 1.png…]()

## 服务组件

1. **Gateway** - HTTP REST API网关
2. **Scheduler** - 任务调度和负载均衡
3. **Worker** - 任务执行（支持多实例）
4. **Tracker** - 任务状态查询服务
5. **Stats** - 系统指标收集服务

## 核心特性

- **服务发现**：基于Etcd的服务注册与发现
- **负载均衡**：轮询、最少连接、一致性哈希
- **熔断器**：自动故障检测与恢复
- **任务重试**：指数退避重试机制
- **优先级队列**：高/中/低优先级任务队列
- **分布式ID**：雪花算法生成任务ID
- **优雅关闭**：SIGINT/SIGTERM信号处理

## 编译项目

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

## 配置说明

编辑 `config/` 目录下的YAML配置文件：

- `gateway.yaml` - 网关服务配置
- `scheduler.yaml` - 调度器服务配置
- `worker.yaml` - 工作节点服务配置
- `tracker.yaml` - 追踪器服务配置
- `stats.yaml` - 统计服务配置

## 运行项目

### 启动依赖服务

```bash
sudo systemctl start etcd
sudo systemctl start redis-server
```

### 启动所有服务

```bash
./scripts/start_all.sh
```

### 停止所有服务

```bash
./scripts/stop_all.sh
```

## API使用

### 提交任务

```bash
curl -X POST http://localhost:8080/task \
  -H "Content-Type: application/json" \
  -d '{"type":"video","priority":2,"data":"test.mp4"}'
```

### 查询任务状态

```bash
curl http://localhost:8080/task?id=<task_id>
```

### 获取Worker列表

```bash
curl http://localhost:8080/workers
```

### 健康检查

```bash
curl http://localhost:8080/health
```

## 任务类型

- `video` - 视频转码任务
- `image` - 图像处理任务
- `data` - 数据导出任务

## 优先级等级

- 0 (低)
- 1 (中)
- 2 (高)

## 项目依赖

- gRPC & Protobuf
- Boost.Asio
- Redis (hiredis)
- Etcd client
- yaml-cpp
- spdlog

## 许可证

MIT
