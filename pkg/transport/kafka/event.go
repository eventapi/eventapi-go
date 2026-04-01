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

// 协议: Kafka (Apache Kafka 3.x)
// 文档: https://kafka.apache.org/documentation/#consumerconfigs
//
// Kafka 使用偏移量提交进行消息确认。
// 处理完成后调用 Commit() 持久化偏移量。
type ReceivedEvent[T any] struct {
	// Event 解析后的 CloudEvent，Data 字段为业务数据
	Event *cloudevent.CloudEvent[T]

	// Commit 提交消费偏移量，表示消息已成功处理。
	// 必须在业务逻辑处理完成后调用，否则消费者重启后消息会重新投递。
	//
	// 使用场景:
	//   1. 正常确认: if err := handler(event); err == nil { re.Commit(ctx) }
	//   2. 结合 panic recover: defer func() { if r := recover(); r != nil { /* 不 Commit */ } }()
	Commit func(context.Context) error

	// Partition 消息所在的分区号。
	//
	// 使用场景:
	//   1. 日志追踪: log.Printf("processing partition=%d offset=%d", re.Partition, re.Offset)
	//   2. 排查卡住的消息: 在监控面板中定位具体分区和偏移量
	Partition int32

	// Offset 消息在分区中的偏移量。
	//
	// 使用场景:
	//   1. 日志追踪: 与 Partition 配合使用，精确定位消息位置
	//   2. 消费进度监控: 将 offset 上报到 Prometheus/Grafana
	//   3. 手动重置偏移量: 结合 kafka.ResetOffset 重新消费指定位置的消息
	Offset int64
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

// WithFailure 标记事件执行失败，原子化写入 error_code 和 error_msg
func (b *EventBuilder[T]) WithFailure(code, msg string) *EventBuilder[T] {
	if b.ce.Extensions == nil {
		b.ce.Extensions = make(cloudevent.Extensions)
	}
	b.ce.Extensions["error_code"] = cloudevent.NewAttributeValue(code)
	b.ce.Extensions["error_msg"] = cloudevent.NewAttributeValue(msg)
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
	_ ...func(ctx context.Context, event *cloudevent.CloudEvent[[]byte], callback func(context.Context) error) error,
) *EventBuilder[T] {
	return &EventBuilder[T]{
		ce:    ce,
		codec: cd,
		send:  sendFn,
	}
}
