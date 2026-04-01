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

// ReceivedEvent AMQP 接收事件
type ReceivedEvent[T any] struct {
	Event       *cloudevent.CloudEvent[T]         // 接收到的 CloudEvent
	Ack         func(context.Context) error       // 确认消息
	Reject      func(context.Context, bool) error // 拒绝消息（requeue: 是否重新入队）
	DeliveryTag uint64                            // 投递标签
	Redelivered bool                              // 是否重投递
	Exchange    string                            // 交换机
	RoutingKey  string                            // 路由键
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
) *EventBuilder[T] {
	return &EventBuilder[T]{
		ce:    ce,
		codec: cd,
		send:  sendFn,
	}
}
