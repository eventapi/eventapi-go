# EventAPI

[![Go Reference](https://pkg.go.dev/badge/github.com/eventapi/eventapi-go.svg)](https://pkg.go.dev/github.com/eventapi/eventapi-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/eventapi/eventapi-go)](https://goreportcard.com/report/github.com/eventapi/eventapi-go)

基于 Protobuf 的异步事件驱动架构，将 CloudEvents 事件模型与 AsyncAPI 拓扑架构融合，提供类型安全的异步通信能力。

## 核心特性

- **类型安全**: Protobuf IDL 定义事件结构，编译期类型检查
- **CloudEvents 规范**: 符合 CloudEvents 1.0.2 标准
- **多协议支持**: IDL 支持多种消息队列协议，一个 Service 可声明多个协议
- **代码生成**: 从 IDL 自动生成客户端 SDK，每个协议独立生成
- **强类型**: 每个协议独立定义 EventBuilder 和 ReceivedEvent
- **链式调用**: 通过 Builder 模式灵活配置协议特定选项

## 消息队列支持

| 协议     | IDL 支持 | Transport 实现 | 说明               |
| -------- | -------- | -------------- | ------------------ |
| Kafka    | ✅       | ✅             | 已完成实现         |
| AMQP     | ✅       | ✅             | 已完成实现         |
| RocketMQ | ✅       | ✅             | 已完成实现         |
| MQTT     | ✅       | ❌             | 待实现             |
| NATS     | ✅       | ❌             | 待实现             |

如需支持其他消息队列，只需实现 `transport.Transport` 接口并提供对应的 EventBuilder。

## 依赖要求

- Go 1.18+ (需要泛型支持)
- Protocol Buffers 编译器 (protoc)
- protoc-gen-go (Go protobuf 插件)
- buf (推荐，用于代码生成)

```bash
# 安装 protoc (macOS)
brew install protobuf

# 安装 Buf (推荐)
brew install bufbuild/buf/buf

# 安装 EventAPI 插件
go install -ldflags="-s -w" github.com/eventapi/eventapi-go/cmd/protoc-gen-eventapi@latest
```

## 快速开始

### 1. 定义事件总线

```protobuf
syntax = "proto3";
package myservice.v1;

import "eventapi/v1/eventapi.proto";

option go_package = "github.com/example/myservice";

// 定义服务，声明支持的协议 (可多个)
service UserEventBus {
  option (eventapi.v1.server) = {
    protocols: [
      { name: "kafka", version: "3.5.0" },
      { name: "amqp", version: "0.9.1" }
    ]
  };

  // 发送操作
  rpc SendUserCreated(UserCreatedEvent) returns (google.protobuf.Empty) {
    option (eventapi.v1.operation) = {
      action: SEND
      channel: {
        address: "user.events"
        binding: {
          kafka: {
            key: "user_id"
          }
        }
      }
    };
  }

  // 接收操作
  rpc ReceiveUserCreated(UserCreatedEvent) returns (google.protobuf.Empty) {
    option (eventapi.v1.operation) = {
      action: RECEIVE
      channel: {
        address: "user.events"
      }
    };
  }
}

// 定义事件，声明 CloudEvent 类型
message UserCreatedEvent {
  option (eventapi.v1.event_type) = "acme.user.created.v1";
  
  string user_id = 1 [(eventapi.v1.is_subject) = true];
  string email = 2;
  int64 created_at = 3;
}
```

### 2. 生成 SDK

#### **使用 Buf (推荐):**

```bash
# 首次需要登录 BSR (如使用远程依赖)
buf registry login

# 生成代码
buf generate
```

#### **使用 protoc:**

```bash
protoc \
  --eventapi_out=. \
  --eventapi_opt=paths=source_relative \
  -I. \
  your_service.proto
```

#### 生成的目录结构：

```
gen/
└── user_event_bus/
    ├── kafka/        # Kafka SDK (package: user_event_bus_kafka)
    │   └── user_event_bus.go
    └── amqp/         # AMQP SDK (package: user_event_bus_amqp)
        └── user_event_bus.go
```

### 3. 使用 SDK

#### 发送事件

```go
import (
    "context"
    "github.com/example/myservice/gen/user_event_bus/kafka"
)

client, err := kafka.NewClient(ctx, kafka.Config{
    Brokers: []string{"localhost:9092"},
    GroupID: "my-consumer-group",
})

// 使用 Builder 链式调用发送事件
err = client.NewSendUserCreated(&kafka.UserCreatedEvent{
    UserId:    "123",
    Email:     "user@example.com",
    CreatedAt: time.Now().Unix(),
}).
    WithSubject("user.123").
    Send(ctx)
```

#### 接收事件

```go
err := client.ReceiveUserCreated(ctx, func(ctx context.Context, received *kafka.ReceivedUserCreated) error {
    event := received.Event.Data  // 已反序列化为 UserCreatedEvent
  
    // 处理事件...
  
    return received.Ack(ctx)  // 提交确认
})
```

### AMQP 示例

```go
import (
    "context"
    "github.com/example/myservice/gen/order_event_bus/amqp"
)

client, err := amqp.NewClient(ctx, amqp.Config{
    URL:     "amqp://guest:guest@localhost:5672/",
    GroupID: "order-consumer-group",
})

// 发送事件 (带 Exchange 绑定)
err = client.NewSendOrder(&amqp.OrderEvent{
    OrderId: "12345",
    Amount:  19900,
}).
    WithExchange("order.exchange").
    WithRoutingKey("order.created").
    Send(ctx)

// 接收事件
err := client.ReceiveOrder(ctx, func(ctx context.Context, received *amqp.ReceivedOrderEvent) error {
    return received.Ack(ctx)
})
```

### RocketMQ 示例

```go
import (
    "context"
    "github.com/example/myservice/gen/order_event_bus/rocketmq"
)

client, err := rocketmq.NewClient(ctx, rocketmq.Config{
    Endpoint:  "localhost:8080",
    GroupID:   "order-producer-group",
    AccessKey: "your-access-key",
    SecretKey: "your-secret-key",
})

// 发送普通消息
err = client.NewSendOrder(&rocketmq.OrderEvent{
    OrderId: "12345",
}).
    Send(ctx)

// 发送延时消息
err = client.NewSendOrder(&rocketmq.OrderEvent{
    OrderId: "12345",
}).
    WithDelay(5 * time.Minute).
    Send(ctx)

// 发送事务消息
err = client.NewSendOrder(&rocketmq.OrderEvent{
    OrderId: "12345",
}).
    WithTransaction(func(ctx context.Context) error {
        // 本地事务逻辑，返回 nil 则 commit，返回 error 则 rollback
        return db.ExecContext(ctx, "INSERT INTO orders ...")
    }).
    Send(ctx)
```

## 架构说明

```
proto/eventapi/v1/          # IDL 定义
cmd/protoc-gen-eventapi/    # 代码生成插件
pkg/
├── eventapi/v1/            # IDL 定义 (proto 转 Go)
├── cloudevent/             # CloudEvent 数据模型
├── transport/              # 传输层抽象
│   ├── transport.go       # 基础接口
│   ├── kafka/             # Kafka 实现
│   │   ├── event.go       # EventBuilder + ReceivedEvent
│   │   └── client.go      # Transport 实现
│   ├── rocketmq/          # RocketMQ 实现
│   │   ├── event.go       # EventBuilder (WithDelay/WithTransaction/WithMessageGroup) + ReceivedEvent
│   │   └── client.go      # Transport 实现
│   └── amqp/              # AMQP 实现
│       ├── event.go       # EventBuilder + ReceivedEvent
│       └── client.go      # Transport 实现
├── codec/                  # 编解码器
└── options/                # 配置选项
```

## 协议特性对比

| 特性       | Kafka                  | RocketMQ                      | AMQP                        |
| ---------- | --------------------- | ------------------------------ | --------------------------- |
| 发送选项   | -                     | WithDelay, WithTransaction    | WithExchange, WithPriority |
| 接收操作   | Ack()                 | Ack()                          | Ack(), Reject()            |
| 接收属性   | Partition, Offset     | MessageID, ReconsumeTimes      | DeliveryTag, Redelivered   |

## IDL 注解说明

| 级别    | 注解           | 用途                        |
| ------- | -------------- | --------------------------- |
| Service | `server.protocols` | 声明支持的协议列表 (可多个) |
| Method  | `operation`     | 声明操作类型和通道          |
| Message | `event_type`   | 声明 CloudEvent 类型       |
| Field   | `is_subject`  | 标记路由键字段              |

## 多协议迁移

当需要从一种消息队列迁移到另一种时，只需修改 Service 声明：

```protobuf
// 迁移前
service OrderEventBus {
  option (eventapi.v1.server) = {
    protocols: [{ name: "kafka", version: "3.5.0" }]
  };
}

// 迁移后
service OrderEventBus {
  option (eventapi.v1.server) = {
    protocols: [{ name: "amqp", version: "0.9.1" }]
  };
}
```

重新生成 SDK 后，代码调用方式不变，只需切换 import 和配置。

## License

MIT