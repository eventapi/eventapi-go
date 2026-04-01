package kafka

import (
	"context"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
)

// 协议: Kafka (Apache Kafka 3.x)
// 文档: https://kafka.apache.org/documentation/#producerconfigs

// Transport 是 Kafka 传输层接口。
//
// 使用示例:
//
//	tr, _ := kafka.New(ctx, kafka.Config{Brokers: []string{"localhost:9092"}})
//	defer tr.Close()
//
//	ch, _ := tr.Channel(kafka.Channel{Address: "order.events"})
//	ch.Send(ctx, event)
type Transport interface {
	// Channel 返回指定 Topic 配置的 TransportChannel。
	// 每次调用创建新的 Channel；Reader 在首次调用 OnReceive 时延迟创建。
	Channel(Channel) (TransportChannel, error)

	// Close 关闭传输层，释放底层 Writer。
	Close() error
}

// TransportChannel 表示单个 Kafka 通道，具备发送和接收能力。
type TransportChannel interface {
	// Send 将 CloudEvent 发布到配置的 Kafka Topic。
	Send(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error

	// OnReceive 注册消息消费处理器。
	// 每条消息调用一次 handler；消费循环在后台 goroutine 中运行。
	// handler 必须调用 re.Commit(ctx) 确认消息偏移量。
	OnReceive(ctx context.Context, handle func(context.Context, *ReceivedEvent[[]byte]) error) error
}
