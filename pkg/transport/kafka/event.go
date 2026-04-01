// Package kafka Kafka 传输层实现
//
// Kafka 使用基于偏移量的消息确认机制
package kafka

import (
	"context"
	"fmt"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	"github.com/eventapi/eventapi-go/pkg/codec"
)

// ReceivedEvent Kafka 接收事件
type ReceivedEvent[T any] struct {
	Event     *cloudevent.CloudEvent[T]   // 接收到的 CloudEvent
	Commit    func(context.Context) error // 提交偏移量
	Partition int32                       // 所在分区
	Offset    int64                       // 消息偏移量
}

// EventBuilder Kafka 事件构建器
type EventBuilder[T any] struct {
	ce    *cloudevent.CloudEvent[T]                                             // CloudEvent
	codec codec.Codec                                                           // 编解码器
	send  func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error // 发送函数
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

// Send 发送事件
func (b *EventBuilder[T]) Send(ctx context.Context) error {
	data, err := b.codec.Encode(b.ce.Data)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	ce := &cloudevent.CloudEvent[[]byte]{
		CloudEventMetadata: b.ce.CloudEventMetadata,
		Data:               data,
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
