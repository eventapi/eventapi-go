package rocketmq

import (
	"context"
	"time"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	"github.com/eventapi/eventapi-go/pkg/codec"
)

// 协议: RocketMQ 5.x
// 文档: https://rocketmq.apache.org/docs/featureBehavior/01consumptionretry/
//
// RocketMQ 使用确认机制进行消息消费。
// 调用 Ack() 确认消费完成，未确认的消息会在超时后重新投递。
type ReceivedEvent[T any] struct {
	// Event 解析后的 CloudEvent，Data 字段为业务数据
	Event *cloudevent.CloudEvent[T]

	// Ack 确认消息已成功消费。调用后 broker 会更新消费进度。
	// 未调用 Ack 的消息会在超时后重新投递。
	//
	// 使用场景:
	//   1. 正常确认: if err := handler(event); err == nil { re.Ack(ctx) }
	//   2. 结合 panic recover: defer func() { if r := recover(); r != nil { /* 不 Ack */ } }()
	Ack func(context.Context) error

	// MessageID 消息唯一标识，可用于日志追踪或幂等校验。
	//
	// 使用场景:
	//   1. 幂等校验: if redis.Exists("processed:" + re.MessageID) { return nil }
	//   2. 日志追踪: log.Printf("processing message %s", re.MessageID)
	//   3. 死信排查: 根据 MessageID 在 RocketMQ 控制台定位消息轨迹
	MessageID string

	// ReconsumeTimes 消息重复消费次数。
	//
	// 使用场景:
	//   1. 重试限制: if re.ReconsumeTimes > 3 { return re.Nack(ctx, false) }
	//   2. 告警触发: 当 ReconsumeTimes > 阈值时发送告警
	//   3. 退避策略: 根据重试次数动态调整处理逻辑
	ReconsumeTimes int32
}

// EventBuilder RocketMQ 事件构建器
type EventBuilder[T any] struct {
	ce             *cloudevent.CloudEvent[T]                                             // CloudEvent
	codec          codec.Codec                                                           // 编解码器
	send           func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error // 发送函数
	sendTx         func(ctx context.Context, event *cloudevent.CloudEvent[[]byte], callback func(context.Context) error) error
	delayTimestamp *time.Time                  // 延时时间
	messageGroup   string                      // 消息组（顺序消息）
	txCallback     func(context.Context) error // 事务回调，非 nil 时走事务发送
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

// WithTransaction 设置事务回调，Send() 时自动走事务发送流程
func (b *EventBuilder[T]) WithTransaction(fn func(context.Context) error) *EventBuilder[T] {
	b.txCallback = fn
	return b
}

func (b *EventBuilder[T]) Send(ctx context.Context) error {
	ce := b.buildCloudEvent()
	if b.txCallback != nil && b.sendTx != nil {
		return b.sendTx(ctx, ce, b.txCallback)
	}
	return b.send(ctx, ce)
}

func (b *EventBuilder[T]) buildCloudEvent() *cloudevent.CloudEvent[[]byte] {
	data, _ := b.codec.Encode(b.ce.Data)
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
	return ce
}

// NewEventBuilder 创建事件构建器
func NewEventBuilder[T any](
	ce *cloudevent.CloudEvent[T],
	cd codec.Codec,
	sendFn func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error,
	sendTx func(ctx context.Context, event *cloudevent.CloudEvent[[]byte], callback func(context.Context) error) error,
) *EventBuilder[T] {
	return &EventBuilder[T]{
		ce:     ce,
		codec:  cd,
		send:   sendFn,
		sendTx: sendTx,
	}
}
