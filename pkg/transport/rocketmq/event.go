// Package rocketmq RocketMQ 传输层实现
//
// 支持延时消息、事务消息、顺序消息
package rocketmq

import (
	"context"
	"fmt"
	"time"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	"github.com/eventapi/eventapi-go/pkg/codec"
)

// ReceivedEvent RocketMQ 接收事件
type ReceivedEvent[T any] struct {
	Event             *cloudevent.CloudEvent[T]   // 接收到的 CloudEvent
	Ack               func(context.Context) error // 确认消息
	MessageID         string                      // 消息ID
	DeliveryTimestamp time.Time                   // 投递时间
	ReconsumeTimes    int32                       // 重复消费次数
}

// EventBuilder RocketMQ 事件构建器
type EventBuilder[T any] struct {
	ce                  *cloudevent.CloudEvent[T]                                             // CloudEvent
	codec               codec.Codec                                                           // 编解码器
	send                func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error // 发送函数
	transactionSend     func(ctx context.Context, event *cloudevent.CloudEvent[[]byte], callback func(context.Context) error) error
	transactionCallback func(context.Context) error
	delayTimestamp      *time.Time // 延时时间
	messageGroup        string     // 消息组（顺序消息）
	isTransaction       bool       // 是否事务消息
}

// WithDelay 设置延时时间
func (b *EventBuilder[T]) WithDelay(delay time.Duration) *EventBuilder[T] {
	ts := time.Now().Add(delay)
	b.delayTimestamp = &ts
	return b
}

// WithDelayAt 设置延时时间点
func (b *EventBuilder[T]) WithDelayAt(t time.Time) *EventBuilder[T] {
	b.delayTimestamp = &t
	return b
}

// WithMessageGroup 设置消息组（顺序消息）
func (b *EventBuilder[T]) WithMessageGroup(group string) *EventBuilder[T] {
	b.messageGroup = group
	return b
}

// WithTransaction 启用事务模式，fn 为本地事务执行函数
func (b *EventBuilder[T]) WithTransaction(fn func(context.Context) error) *EventBuilder[T] {
	b.isTransaction = true
	b.transactionCallback = fn
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

	if b.delayTimestamp != nil {
		ce.Extensions["delay_timestamp"] = cloudevent.NewAttributeValue(b.delayTimestamp.UnixMilli())
	}
	if b.messageGroup != "" {
		ce.Extensions["message_group"] = cloudevent.NewAttributeValue(b.messageGroup)
	}
	if b.isTransaction {
		ce.Extensions["transaction"] = cloudevent.NewAttributeValue(true)
	}

	if b.isTransaction && b.transactionCallback != nil && b.transactionSend != nil {
		return b.transactionSend(ctx, ce, b.transactionCallback)
	}
	return b.send(ctx, ce)
}

// NewEventBuilder 创建事件构建器
func NewEventBuilder[T any](
	ce *cloudevent.CloudEvent[T],
	cd codec.Codec,
	sendFn func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error,
	transactionSendFn ...func(ctx context.Context, event *cloudevent.CloudEvent[[]byte], callback func(context.Context) error) error,
) *EventBuilder[T] {
	b := &EventBuilder[T]{
		ce:    ce,
		codec: cd,
		send:  sendFn,
	}
	if len(transactionSendFn) > 0 {
		b.transactionSend = transactionSendFn[0]
	}
	return b
}
