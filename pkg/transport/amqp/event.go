// Package amqp AMQP 传输层实现
//
// 支持消息优先级、交换机、路由键等特性
package amqp

import (
	"context"
	"fmt"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	"github.com/eventapi/eventapi-go/pkg/codec"
)

// 协议: AMQP 0.9.1 (RabbitMQ)
// 文档: https://www.rabbitmq.com/tutorials/amqp-concepts.html
//
// AMQP 使用显式确认机制 (basic.ack / basic.nack)。
// 调用 Ack() 确认处理完成，或 Nack(requeue) 拒绝消息。
type ReceivedEvent[T any] struct {
	// Event 解析后的 CloudEvent，Data 字段为业务数据
	Event *cloudevent.CloudEvent[T]

	// Ack 确认消息已成功处理。调用后 broker 会从队列中删除该消息。
	// 必须在业务逻辑处理完成后调用，否则消息会在消费者重启后重新投递。
	//
	// 使用场景:
	//   1. 正常确认: if err := handler(event); err == nil { re.Ack(ctx) }
	//   2. 结合 panic recover: 在 defer 中判断是否需要 Nack
	Ack func(context.Context) error

	// Nack 拒绝消息。requeue=true 时消息会重新入队等待投递；
	// requeue=false 时消息会被丢弃或进入死信队列（如果配置了 DLX）。
	//
	// 使用场景:
	//   1. 临时错误重试: re.Nack(ctx, true) - 消息重新入队，等待下次消费
	//   2. 永久错误/毒消息: re.Nack(ctx, false) - 进入死信队列，人工介入
	//   3. 结合错误类型判断:
	//      if isRetryable(err) { re.Nack(ctx, true) } else { re.Nack(ctx, false) }
	Nack func(context.Context, bool) error
}

// EventBuilder AMQP 事件构建器
type EventBuilder[T any] struct {
	ce         *cloudevent.CloudEvent[T]                                             // CloudEvent
	codec      codec.Codec                                                           // 编解码器
	send       func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error // 发送函数
	exchange   string                                                                // 交换机
	routingKey string                                                                // 路由键
	priority   int8                                                                  // 优先级
}

// WithExchange 设置交换机
func (b *EventBuilder[T]) WithExchange(exchange string) *EventBuilder[T] {
	b.exchange = exchange
	return b
}

// WithRoutingKey 设置路由键
func (b *EventBuilder[T]) WithRoutingKey(key string) *EventBuilder[T] {
	b.routingKey = key
	return b
}

// WithPriority 设置消息优先级
func (b *EventBuilder[T]) WithPriority(priority int8) *EventBuilder[T] {
	b.priority = priority
	return b
}

// WithExtensions 设置扩展属性
func (b *EventBuilder[T]) WithExtensions(ext cloudevent.Extensions) *EventBuilder[T] {
	if ext == nil {
		ext = make(cloudevent.Extensions)
	}
	b.ce.Extensions = ext
	return b
}

// WithSubject 设置 CloudEvent subject
func (b *EventBuilder[T]) WithSubject(subject string) *EventBuilder[T] {
	b.ce.Subject = subject
	return b
}

// WithFailure 标记事件执行失败，原子化写入 error_code 和 error_msg
func (b *EventBuilder[T]) WithFailure(code, msg string) *EventBuilder[T] {
	if b.ce.Extensions == nil {
		b.ce.Extensions = make(cloudevent.Extensions)
	}
	b.ce.Extensions["error_code"] = cloudevent.NewAttributeValue(code)
	b.ce.Extensions["error_msg"] = cloudevent.NewAttributeValue(msg)
	return b
}

func (b *EventBuilder[T]) Send(ctx context.Context) error {
	data, err := b.codec.Encode(b.ce.Data)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	ce := &cloudevent.CloudEvent[[]byte]{
		CloudEventMetadata: b.ce.CloudEventMetadata,
		Data:               data,
	}
	ce.Extensions = make(cloudevent.Extensions)

	if b.exchange != "" {
		ce.Extensions["amqp_exchange"] = cloudevent.NewAttributeValue(b.exchange)
	}
	if b.routingKey != "" {
		ce.Extensions["amqp_routing_key"] = cloudevent.NewAttributeValue(b.routingKey)
	}
	if b.priority != 0 {
		ce.Extensions["amqp_priority"] = cloudevent.NewAttributeValue(b.priority)
	}

	return b.send(ctx, ce)
}

// NewEventBuilder 创建事件构建器
func NewEventBuilder[T any](
	ce *cloudevent.CloudEvent[T],
	cd codec.Codec,
	sendFn func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error,
	_ ...func(ctx context.Context, event *cloudevent.CloudEvent[[]byte], callback func(context.Context) error) error,
) *EventBuilder[T] {
	return &EventBuilder[T]{
		ce:    ce,
		codec: cd,
		send:  sendFn,
	}
}
