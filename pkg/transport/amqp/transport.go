package amqp

import (
	"context"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
)

// 协议: AMQP 0.9.1 (RabbitMQ)
// 文档: https://www.rabbitmq.com/tutorials/amqp-concepts.html

// Transport 是 AMQP 传输层接口。
//
// 使用示例:
//
//	tr, _ := amqp.New(ctx, amqp.Config{URL: "amqp://localhost:5672"})
//	defer tr.Close()
//
//	ch, _ := tr.Channel(amqp.Channel{Address: "order.queue"})
//	ch.Send(ctx, event)
type Transport interface {
	// Channel 返回指定 Queue 配置的 TransportChannel。
	// Channel 按地址缓存；相同地址的重复调用返回同一实例。
	Channel(Channel) (TransportChannel, error)

	// Close 关闭传输层，排空 Channel 池并关闭 AMQP 连接。
	Close() error
}

// TransportChannel 表示单个 AMQP 通道，具备发送和接收能力。
type TransportChannel interface {
	// Send 将 CloudEvent 发布到配置的 Queue 或 Exchange。
	// 如果配置了 Exchange 绑定，会在发送前自动声明 Exchange、Queue 和绑定关系。
	Send(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error

	// OnReceive 注册消息消费处理器。
	// 每次投递调用一次 handler；消费循环在后台 goroutine 中运行。
	// handler 必须调用 re.Ack(ctx) 或 re.Nack(ctx, requeue) 确认投递。
	OnReceive(ctx context.Context, handle func(context.Context, *ReceivedEvent[[]byte]) error) error
}
