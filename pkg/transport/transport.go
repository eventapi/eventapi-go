// Package transport 提供事件传输层的抽象接口
//
// 设计原则:
// 1. 泛型设计 - Transport[T Channel] 允许不同类型通道（Kafka Topic、AMQP Queue等）
// 2. 配置驱动行为 - 具体传输行为由 Config 决定（同步/异步、批处理等）
// 3. 确认机制 - 支持手动 ACK/NACK，保证消息可靠性
// 4. 类型安全 - Channel 作为泛型参数，编译期保证类型正确
//
// 使用示例:
//
//	// Kafka 配置
//	cfg := kafkatransport.Config{
//	    Brokers:   []string{"localhost:9092"},
//	    Async:     true,  // 异步批处理
//	    BatchSize: 100,
//	}
//
//	// 创建 Transport
//	transport, _ := kafkatransport.New(cfg)
//
//	// 获取 Channel
//	channel := transport.Channel(kafkatransport.Channel{Name: "user.events"})
//
//	// 发送事件
//	channel.Send(ctx, &cloudevent.CloudEvent{...})
//
//	// 接收事件
//	channel.OnReceive(ctx, func(ctx context.Context, event *transport.ReceivedEvent) error {
//	    // 处理事件
//	    return event.Ack(ctx, true) // 确认消息
//	})
package transport

import (
	"context"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
)

// Channel 是通道标识接口
// 具体实现包括 Kafka Topic、AMQP Queue 等
type Channel interface{}

// Transport 是传输层的抽象接口
// 泛型参数 T 是具体的 Channel 类型
// 通过泛型实现类型安全，避免运行时类型断言
type Transport[T Channel] interface {
	// Channel 返回指定通道的 TransportChannel
	// 每个 Channel 对应一个独立的传输通道
	Channel(T) (TransportChannel, error)

	// Close 关闭传输层，释放资源
	// 应该优雅地关闭所有活跃连接
	Close() error
}

// TransportChannel 是单个通道的传输接口
// 提供发送和接收能力
type TransportChannel interface {
	Sender
	OnReceiver
}

type Sender interface {
	// Send 发送 CloudEvent 到通道
	// 具体行为（同步/异步、批处理）由 Transport 的 Config 决定
	Send(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error
}

type OnReceiver interface {
	OnReceive(ctx context.Context, handle OnReceiveHandle[[]byte]) error
}

type OnReceiveHandle[T any] func(ctx context.Context, event *ReceivedEvent[T]) error

type ReceivedEvent[T any] struct {
	Event *cloudevent.CloudEvent[T]
	Ack   func(context.Context) error
}
