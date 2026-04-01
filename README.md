# EventAPI

[![Go Reference](https://pkg.go.dev/badge/github.com/eventapi/eventapi-go.svg)](https://pkg.go.dev/github.com/eventapi/eventapi-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/eventapi/eventapi-go)](https://goreportcard.com/report/github.com/eventapi/eventapi-go)

基于 Protobuf 的**事件驱动架构 IDL**，一份定义同时驱动**服务通信**和**数据集成**。

## 解决的问题

### 通信问题

| 痛点                                                | 解法                                                  |
| --------------------------------------------------- | ----------------------------------------------------- |
| 定义分散：事件结构散落在代码、DB 表、Flink SQL 三处 | Proto 是唯一事实来源，SDK/DDL/SQL 全部自动生成        |
| 通信无契约：Topic、分区规则靠口头约定               | `operation` 声明通信拓扑，编译期校验                |
| 换 MQ 成本高：换消息队列要重写收发代码              | 同一份 proto，切换 `protocol` 字段值 即可生成新 SDK |
| 元数据混乱：Event 头信息各团队自行处理              | SDK 自动填充 CloudEvent 标准头                        |

### 数据集成问题

| 痛点                                               | 解法                                           |
| -------------------------------------------------- | ---------------------------------------------- |
| SQL 手写易错：字段映射易错，schema 变更 SQL 不更新 | `sink` 声明字段映射，proto 改了 SQL 自动更新 |
| 集成配置分散：DDL、Flink SQL、Connector 各写各的   | 一份 `sink` 声明，同时生成 DDL + Flink SQL   |

## 架构定位

```text
                    Proto 定义 (唯一事实来源)
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
     protoc-gen-    protoc-gen-      其他插件
      eventapi      eventapi-sink
            │              │
            ▼              ▼
     Go SDK 代码      DDL + Flink SQL
   (Kafka/AMQP/      (StarRocks/MySQL/
    RocketMQ)         ClickHouse)
            │              │
            ▼              ▼
     服务间通信        数据集成 Pipeline
```

## 快速开始

### 安装

```bash
brew install bufbuild/buf/buf
go install -ldflags="-s -w" github.com/eventapi/eventapi-go/cmd/protoc-gen-eventapi@latest
go install -ldflags="-s -w" github.com/eventapi/eventapi-go/cmd/protoc-gen-eventapi-sink@latest
```

### 1. 定义

一份 proto 同时声明**通信拓扑**和**数据集成**：

```protobuf
syntax = "proto3";
package myservice.v1;

// 引入 EventAPI 核心注解
import "eventapi/v1/eventapi.proto";     // server / operation 注解
import "eventapi/v1/cloudevent.proto";   // event_type / event_data_content_type / is_event_subject 注解
import "eventapi/v1/sink.proto";         // sink 注解 (数据集成)
import "google/protobuf/empty.proto";

// 定义事件总线服务
service UserEventBus {
  // 声明支持的协议 (可多个: kafka, amqp, rocketmq)
  option (eventapi.v1.server) = {
    protocols: [{ name: "kafka", version: "3.5.0" }]
  };

  // 发送操作: 定义消息发往哪个 Topic
  rpc SendUserCreated(UserCreatedEvent) returns (google.protobuf.Empty) {
    // operation 声明通信拓扑: 操作类型 + 通道地址
    option (eventapi.v1.operation) = {
      action: SEND
      channel: { address: "user.events" }
    };
    // sink 声明数据集成: 事件如何落入下游存储
    option (eventapi.v1.sink) = {
      target: SINK_TYPE_STARROCKS          // 目标数据库
      database: "analytics"                // 数据库名
      table: "user_events"                 // 表名
      source_topic: "user.events"          // 源 Kafka Topic
      event_data_content_type: CONTENT_TYPE_JSON  // 数据编码格式
      primary_key: ["event_id"]            // 主键
      partition_key: ["event_time"]        // 分区列
      mapping: [
        // header_path: 从 CloudEvent 头部提取（默认补 ce_ 前缀）
        { header_path: "id",      target_column: "event_id",   type: COLUMN_TYPE_VARCHAR, not_null: true },
        { header_path: "time",    target_column: "event_time", type: COLUMN_TYPE_TIMESTAMP },

        // 嵌套提取: header 值是 JSON 时
        { header_path: "$.ce_source.tenant", target_column: "tenant_id", type: COLUMN_TYPE_VARCHAR },

        // 非 ce_ 开头的 Kafka Header，用 raw: 跳过自动补全
        { header_path: "raw:x-custom-trace", target_column: "custom_trace", type: COLUMN_TYPE_VARCHAR },

        // data 字段自动推断，无需手动写
        // 关闭自动推断: auto_infer_data_mapping: false
      ]
    };
  }

  // 接收操作: 定义从哪个 Topic 消费消息
  rpc ReceiveUserCreated(UserCreatedEvent) returns (google.protobuf.Empty) {
    option (eventapi.v1.operation) = {
      action: RECEIVE
      channel: { address: "user.events" }
    };
  }
}

// 定义事件结构
message UserCreatedEvent {
  // event_type: 声明 CloudEvent type 属性
  option (eventapi.v1.event_type) = "acme.user.created.v1";
  // event_data_content_type: 声明数据编码格式
  option (eventapi.v1.event_data_content_type) = CONTENT_TYPE_JSON;

  // is_event_subject: 标记为 CloudEvent subject (路由键)
  string user_id = 1 [(eventapi.v1.is_event_subject) = true];
  string email = 2;
}
```

### 2. 生成

```bash
# Go SDK + DDL + Flink SQL
make generate
```

**生成的 Go SDK 节选** (以 Kafka 为例):

```go
// gen/user_event_bus/kafka/user_event_bus.go

// 事件类型
type UserCreatedEvent struct {
    UserId string `json:"user_id"`  // CloudEvent subject
    Email  string `json:"email"`
}

// 接收到的事件 (Kafka 特有 Commit + Partition + Offset)
type ReceivedUserCreated struct {
    Event     *cloudevent.CloudEvent[*UserCreatedEvent]
    Commit    func(context.Context) error  // 提交偏移量
    Partition int32                        // 分区号
    Offset    int64                        // 偏移量
}

// 客户端接口
type Client interface {
    NewSendUserCreated(event *UserCreatedEvent) *UserCreatedEventBuilder
    ReceiveUserCreated(ctx context.Context, handle func(context.Context, *ReceivedUserCreated) error) error
}
```

**生成的 Flink SQL 节选**:

```sql
-- gen/sink/user_events_flink.sql

-- Source table (JSON format, Flink 自动反序列化)
CREATE TEMPORARY TABLE kafka_source (
  ce_id        STRING METADATA FROM 'header.ce_id' VIRTUAL,
  ce_type      STRING METADATA FROM 'header.ce_type' VIRTUAL,
  ce_source    STRING METADATA FROM 'header.ce_source' VIRTUAL,
  ce_time      STRING METADATA FROM 'header.ce_time' VIRTUAL,
  ce_subject   STRING METADATA FROM 'header.ce_subject' VIRTUAL,
  tenant_id    STRING COMMENT '从 ce_source JSON 中提取',
  user_id      VARCHAR(256) NOT NULL COMMENT 'CloudEvent subject',
  email        VARCHAR(256)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user.events',
  'format' = 'json'
);

-- Sink: StarRocks
CREATE TABLE starrocks_sink (
  event_id     VARCHAR(256) NOT NULL,
  event_type   VARCHAR(256),
  event_source VARCHAR(256),
  event_time   DATETIME,
  subject      VARCHAR(256),
  tenant_id    VARCHAR(256),
  user_id      VARCHAR(256) NOT NULL COMMENT 'CloudEvent subject',
  email        VARCHAR(256),
  PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
  'connector' = 'starrocks',
  'database-name' = 'analytics',
  'table-name' = 'user_events'
);

-- Pipeline
INSERT INTO starrocks_sink
SELECT
  ce_id, ce_type, ce_source, ce_time, ce_subject,
  JSON_VALUE(ce_source, '$.tenant') AS tenant_id,
  user_id, email
FROM kafka_source;
```

### 3. 使用

各协议 SDK 通过 Builder 模式构建客户端，支持链式调用配置中间件、扩展元数据和启用/禁用开关：

**Kafka**:

```go
import "myservice/v1/kafka"

client, _ := kafka.NewClientBuilder(kafka.Config{
    Brokers: []string{"localhost:9092"},
    GroupID: "my-consumer-group",
    Enabled: true,  // 启用传输通道，false 时 Send/Receive 静默跳过
}).
    WithExtensionsProvider(func(ctx context.Context) cloudevent.Extensions {
        return cloudevent.Extensions{
            "trace_id": cloudevent.NewAttributeValue(traceID),
        }
    }).
    WithSource("my-service").
    WithWriterMiddleware(kmw.SendLogger(slog.LevelInfo)).
    WithReaderMiddleware(kmw.ReceiveLogger(slog.LevelInfo)).
    Build(ctx)

// 发送（链式调用）
builder := client.NewSendUserCreated(&kafka.UserCreatedEvent{
    UserId: "123",
    Email:  "user@example.com",
}).
    WithSubject("user-123").
    WithExtensions(cloudevent.Extensions{
        "correlation_id": cloudevent.NewAttributeValue("corr-001"),
    }).
    Send(ctx)

// 标记失败
client.NewSendUserCreated(&kafka.UserCreatedEvent{UserId: "123"}).
    WithFailure("ERR_TIMEOUT", "Email service timeout").
    Send(ctx)

// 接收
client.ReceiveUserCreated(ctx, func(ctx context.Context, r *kafka.ReceivedUserCreated) error {
    // 处理事件...
    return r.Commit(ctx)  // Kafka 提交偏移量
})
```

**AMQP**:

```go
import "myservice/v1/amqp"

client, _ := amqp.NewClientBuilder(amqp.Config{
    URL:     "amqp://guest:guest@localhost:5672/",
    Enabled: true,
}).Build(ctx)

// 接收
client.ReceiveUserCreated(ctx, func(ctx context.Context, r *amqp.ReceivedUserCreated) error {
    // 处理事件...
    return r.Ack(ctx)  // AMQP 确认投递
})
```

**RocketMQ**:

```go
import "myservice/v1/rocketmq"

client, _ := rocketmq.NewClientBuilder(rocketmq.Config{
    Endpoint:  "localhost:8080",
    AccessKey: "ak",
    SecretKey: "sk",
    Enabled:   true,
}).Build(ctx)

// 接收
client.ReceiveUserCreated(ctx, func(ctx context.Context, r *rocketmq.ReceivedUserCreated) error {
    // 处理事件...
    return r.Ack(ctx)  // RocketMQ 确认消费
})
```

---

## 详细介绍

### 项目布局

```
proto/eventapi/v1/              # IDL 定义
  ├── cloudevent.proto          # CloudEvent 核心定义
  ├── eventapi.proto            # Transport 注解
  └── sink.proto                # Sink 注解 + 字段映射

cmd/
  ├── protoc-gen-eventapi       # Go SDK 生成插件
  └── protoc-gen-eventapi-sink  # SQL/DDL 生成插件

pkg/
  ├── transport/                # 传输层（各协议独立）
  │   ├── kafka/                # Kafka 实现
  │   │   ├── transport.go      # Transport + TransportChannel 接口
  │   │   ├── client.go         # 实现
  │   │   └── event.go          # ReceivedEvent (Commit, Partition, Offset)
  │   ├── rocketmq/             # RocketMQ 实现
  │   │   ├── transport.go      # Transport + TransportChannel 接口
  │   │   ├── client.go         # 实现
  │   │   └── event.go          # ReceivedEvent (Ack, MessageID, ReconsumeTimes)
  │   └── amqp/                 # AMQP 实现
  │       ├── transport.go      # Transport + TransportChannel 接口
  │       ├── client.go         # 实现
  │       └── event.go          # ReceivedEvent (Ack, Nack)
  ├── cloudevent/               # CloudEvent 数据模型 + ExtensionsProvider
  ├── codec/                    # 编解码器
  └── uuid/                     # UUID 生成
```

### 中间件

Kafka 传输层支持中间件，可观察/修改底层 `kafkago.Message`：

```go
import kmw "github.com/eventapi/eventapi-go/pkg/transport/kafka/middleware"

c, _ := kafka.NewClientBuilder(kafka.Config{
    Brokers: []string{"localhost:9092"},
}).
    WithWriterMiddleware(kmw.SendLogger(slog.LevelInfo)).
    WithReaderMiddleware(kmw.ReceiveLogger(slog.LevelInfo)).
    Build(ctx)
```

内置中间件：

| 中间件                             | 功能                                               |
| ---------------------------------- | -------------------------------------------------- |
| `SendLogger(level)`              | 打印发送消息的 Topic、Headers、Body 和耗时         |
| `ReceiveLogger(level)`           | 打印接收消息的 Topic、Headers、Body、Offset 和耗时 |
| `SendRetry(maxRetries, backoff)` | 发送失败重试                                       |

#### 链式调用

所有 Builder 方法支持链式调用，可组合配置多个选项：

```go
c, _ := kafka.NewClientBuilder(kafka.Config{
    Brokers: []string{"localhost:9092"},
    GroupID: "order-consumer",
    Enabled: true,  // 启用传输通道
}).
    WithSource("order-service").
    WithExtensionsProvider(traceExtFn).
    WithWriterMiddleware(kmw.SendLogger(slog.LevelInfo)).
    WithReaderMiddleware(kmw.ReceiveLogger(slog.LevelInfo)).
    Build(ctx)
```

所有 `WithXxx` 方法均返回 `*ClientBuilder`，可按任意顺序链接调用。Config 中设置的值不会被 Builder 默认值覆盖，可在 Config 中直接配置或通过 `WithXxx` 方法链式设置。

#### Enabled 开关

`Config.Enabled` 控制传输通道是否启用：

```go
// 启用：正常发送和接收
kafka.Config{
    Brokers: []string{"localhost:9092"},
    Enabled: true,
}

// 不启用：Send() 静默返回 nil，OnReceive() 不启动消费者
kafka.Config{
    Brokers: []string{"localhost:9092"},
    Enabled: false,  // Go bool 零值
}
```

适用于开发/测试环境，需要代码编译通过但又不想真正连接消息队列的场景。

> **注意:** Go bool 零值为 `false`，必须显式设为 `true` 才能启用传输通道。

### 服务间通信

#### 消息队列

| 协议     | IDL | Transport | 说明   |
| -------- | --- | --------- | ------ |
| Kafka    | ✅  | ✅        | 已完成 |
| AMQP     | ✅  | ✅        | 已完成 |
| RocketMQ | ✅  | ✅        | 已完成 |
| MQTT     | ✅  | ❌        | 待实现 |
| NATS     | ✅  | ❌        | 待实现 |

#### 协议特定功能

| 协议               | 功能          | 示例                                 |
| ------------------ | ------------- | ------------------------------------ |
| **Kafka**    | 偏移量提交    | `re.Commit(ctx)`                   |
|                    | 分区信息      | `re.Partition`, `re.Offset`      |
| **RocketMQ** | 延时消息      | `.WithDelay(5 * time.Minute)`      |
|                    | 事务消息      | `.WithTransaction(fn)`             |
|                    | 顺序消息      | `.WithMessageGroup("order-123")`   |
| **AMQP**     | Exchange 绑定 | `.WithExchange("order.exchange")`  |
|                    | 路由键        | `.WithRoutingKey("order.created")` |
|                    | 优先级        | `.WithPriority(5)`                 |
|                    | 拒绝重入队    | `re.Nack(ctx, true)`               |

#### 多协议迁移

切换消息队列只需修改 Service 声明：

```protobuf
// Kafka → AMQP
service OrderEventBus {
  option (eventapi.v1.server) = {
    protocols: [{ name: "amqp", version: "0.9.1" }]
  };
}
```

重新生成 SDK 后，代码调用方式不变，只需切换 import 和配置。

### 数据集成 Pipeline

#### 数据集成目标

| 目标       | DDL | Flink SQL | 说明   |
| ---------- | --- | --------- | ------ |
| StarRocks  | ✅  | ✅        | 已完成 |
| MySQL      | ✅  | ✅        | 已完成 |
| ClickHouse | ✅  | ✅        | 已完成 |
| PostgreSQL | ✅  | ✅        | 已完成 |

#### header_path 规则

`header_path` 默认自动补全 `ce_` 前缀，减少重复书写：

| 写法                                              | 解析结果        | 说明                  |
| ------------------------------------------------- | --------------- | --------------------- |
| `"id"`                                          | `ce_id`       | 默认补 `ce_`        |
| `"trace_id"`                                    | `ce_trace_id` | 默认补 `ce_`        |
| `"ce_time"`                                     | `ce_time`     | 已有前缀，不变        |
| `"raw:x-custom"`                                | `x-custom`    | `raw:` 跳过自动补全 |
| `"$.ce_source.tenant"` | `$.ce_source.tenant` | JSON 路径，不变 |                       |

#### 自动推断

默认开启，只需写 header 映射，data 字段从消息定义自动推断（含类型和注释）。

关闭自动推断，完全手动控制：

```protobuf
option (eventapi.v1.sink) = {
  auto_infer_data_mapping: false
  mapping: [
    { header_path: "ce_id", target_column: "event_id", type: COLUMN_TYPE_VARCHAR },
    { data_path: "$.user_id", target_column: "uid", type: COLUMN_TYPE_VARCHAR }
    // 必须手动写全所有字段
  ]
};
```

---

### IDL 注解参考

| 级别    | 注解                        | 用途                         |
| ------- | --------------------------- | ---------------------------- |
| Service | `server.protocols`        | 声明支持的协议列表           |
| Method  | `operation`               | 声明操作类型和通道           |
| Method  | `sink`                    | 声明数据集成目标             |
| Message | `event_type`              | 声明 CloudEvent type 属性    |
| Message | `event_data_content_type` | 声明数据编码格式             |
| Field   | `is_event_subject`        | 标记 CloudEvent subject 字段 |

---

## License

MIT
