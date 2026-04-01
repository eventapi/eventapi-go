package rocketmq

import (
	"context"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
)

// 协议: RocketMQ 5.x
// 文档: https://rocketmq.apache.org/docs/

// Transport 是 RocketMQ 传输层接口。
//
// 使用示例:
//
//	tr, _ := rocketmq.New(ctx, rocketmq.Config{
//	    Endpoint:  "localhost:8081",
//	    AccessKey: "ak",
//	    SecretKey: "sk",
//	})
//	defer tr.Close()
//
//	ch, _ := tr.Channel(rocketmq.Channel{Address: "order.topic"})
//	ch.Send(ctx, event)
type Transport interface {
	// Channel 返回指定 Topic 配置的 TransportChannel。
	// SimpleConsumer 在首次调用 OnReceive 时延迟创建。
	Channel(Channel) (TransportChannel, error)

	// Close 关闭传输层，停止所有消费者和生产者。
	Close() error
}

// TransportChannel 表示单个 RocketMQ 通道，具备发送和接收能力。
type TransportChannel interface {
	// Send 将 CloudEvent 发布到配置的 Topic。
	// 消息类型（普通/延时/顺序）由 Channel 的 binding 配置决定。
	Send(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error

	// SendTransaction 发送事务消息，携带本地事务回调函数。
	// 消息根据回调结果提交或回滚。
	SendTransaction(ctx context.Context, event *cloudevent.CloudEvent[[]byte], callback func(context.Context) error) error

	// OnReceive 注册消息消费处理器。
	// 每批消息调用一次 handler；消费循环在后台 goroutine 中运行。
	// handler 必须调用 re.Ack(ctx) 确认消费。
	OnReceive(ctx context.Context, handle func(context.Context, *ReceivedEvent[[]byte]) error) error
}
