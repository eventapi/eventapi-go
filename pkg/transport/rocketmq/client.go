// Package rocketmq RocketMQ 传输层实现
//
// # 设计思想
//
// RocketMQ 5.x 使用 SimpleConsumer 拉取模式进行消费。消费者主动调用 Receive() 拉取消息，
// 处理完成后调用 Ack() 确认消费进度。未确认的消息在不可见时间超时后会自动重新投递。
//
// # 架构
//
// rocketmqTransport 管理一个 Producer 和多个 SimpleConsumer（按 Topic 懒创建）。
// 发送支持 4 种消息类型：普通、延时、顺序、事务。
// 事务消息通过 SendTransaction() 方法发送，内部使用 RocketMQ 半消息 + 本地事务 + 提交/回滚流程。
//
// # 接收模型
//
// OnReceive 为每个 Topic 懒创建 SimpleConsumer，启动独立的 receiveLoop goroutine。
// receiveLoop 使用指数退避重试 Receive()，避免网络抖动导致消费中断。
// 每条消息携带 MessageID（用于幂等校验）和 ReconsumeTimes（重试次数）。
package rocketmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/apache/rocketmq-clients/golang"
	"github.com/apache/rocketmq-clients/golang/credentials"
	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	eventapiv1 "github.com/eventapi/eventapi-go/pkg/eventapi/v1"
)

const (
	cePrefix = "ce-"

	defaultMaxRetry          = 3
	defaultBatchSize         = 1
	defaultInvisibleDuration = 30 * time.Second
)

var errEmptyTopic = errors.New("channel address (topic) is empty")
var errEmptyEndpoint = errors.New("endpoint is required")

// Config RocketMQ 传输层配置。
//
// 使用示例:
//
//	// 基本配置（阿里云 RocketMQ）
//	cfg := rocketmq.Config{
//	    Endpoint:  "rmq-cn-xxx.rocketmq.aliyuncs.com:8080",
//	    AccessKey: "your-access-key",
//	    SecretKey: "your-secret-key",
//	}
//
//	// 带消费者配置
//	cfg := rocketmq.Config{
//	    Endpoint:          "localhost:8080",
//	    GroupID:           "GID_my_group",
//	    MaxRetry:          5,
//	    InvisibleDuration: 60 * time.Second,
//	}
//
//	// 带事务回查
//	cfg := rocketmq.Config{
//	    Endpoint: "localhost:8080",
//	    TransactionChecker: &rocketmq.TransactionChecker{
//	        Check: func(msg *rocketmq.MessageView) rocketmq.TransactionResolution {
//	            // 检查本地事务状态，返回 Commit/Rollback/Unknown
//	            return rocketmq.TransactionResolution_COMMIT
//	        },
//	    },
//	}
type Config struct {
	// Endpoint RocketMQ 服务端地址，必填。
	// 格式: "host:port"
	// 示例: "localhost:8080" 或 "rmq-cn-xxx.rocketmq.aliyuncs.com:8080"
	Endpoint string

	// GroupID 消费者组 ID。
	// 接收时必须配置；仅发送时可为空。
	// 阿里云 RocketMQ 格式: "GID_xxx"
	GroupID string

	// AccessKey 访问密钥。阿里云等云服务必填。
	AccessKey string

	// SecretKey 访问密钥。阿里云等云服务必填。
	SecretKey string

	// Namespace 命名空间。
	// 阿里云 RocketMQ 实例 ID，如 "rmq-cn-xxx"。
	Namespace string

	// SendTimeout 发送超时时间。默认 0（使用 SDK 默认值）。
	SendTimeout time.Duration

	// MaxRetry 发送最大重试次数。默认 3。
	MaxRetry int

	// Async 是否启用异步发送。
	// true: 发送后立即返回，不等待 broker 确认。
	Async bool

	// BatchSize 每次拉取的消息数量。默认 1。
	// 值越大吞吐越高，但单条消息处理延迟增加。
	BatchSize int

	// AwaitDuration 拉取消息时的最长等待时间。默认 0（使用 SDK 默认值）。
	AwaitDuration time.Duration

	// InvisibleDuration 消息拉取后的不可见时间（消费超时时间）。默认 30s。
	// 超过此时间未 Ack 的消息会自动重新投递。
	InvisibleDuration time.Duration

	// TransactionChecker 事务消息回查处理器。
	// 当 Producer 重启或网络中断时，RocketMQ 会调用此函数检查半消息的本地事务状态。
	TransactionChecker *TransactionChecker
}

// TransactionChecker 事务消息回查配置。
type TransactionChecker struct {
	// Check 回查函数。根据本地事务状态返回 Commit、Rollback 或 Unknown。
	Check func(msg *golang.MessageView) golang.TransactionResolution
}

type MessageView = golang.MessageView
type Channel = eventapiv1.OperationRule_Channel

type rocketmqTransport struct {
	config    Config
	producer  golang.Producer
	consumers map[string]golang.SimpleConsumer
	mu        sync.RWMutex
}

type rocketmqChannel struct {
	transport *rocketmqTransport
	channel   Channel
}

// New 创建 RocketMQ Transport。
//
// 启动 Producer，SimpleConsumer 在首次调用 OnReceive 时懒创建。
// 如果配置了 TransactionChecker，会自动注册事务回查逻辑。
func New(ctx context.Context, cfg Config) (Transport, error) {
	if cfg.Endpoint == "" {
		return nil, errEmptyEndpoint
	}

	rmqCfg := &golang.Config{
		Endpoint:      cfg.Endpoint,
		NameSpace:     cfg.Namespace,
		ConsumerGroup: cfg.GroupID,
	}

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		rmqCfg.Credentials = &credentials.SessionCredentials{
			AccessKey:    cfg.AccessKey,
			AccessSecret: cfg.SecretKey,
		}
	}

	opts := buildProducerOptions(cfg)

	p, err := golang.NewProducer(rmqCfg, opts...)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create producer: %w", err))
	}

	if err := p.Start(); err != nil {
		return nil, errors.Join(fmt.Errorf("start producer: %w", err))
	}

	return &rocketmqTransport{
		config:    cfg,
		producer:  p,
		consumers: make(map[string]golang.SimpleConsumer),
	}, nil
}

func buildProducerOptions(cfg Config) []golang.ProducerOption {
	opts := []golang.ProducerOption{
		golang.WithMaxAttempts(int32(defaultIf(cfg.MaxRetry, defaultMaxRetry))),
	}

	if cfg.TransactionChecker != nil {
		opts = append(opts, golang.WithTransactionChecker(&golang.TransactionChecker{
			Check: cfg.TransactionChecker.Check,
		}))
	}

	return opts
}

// Channel 为给定配置创建一个新的 RocketMQ 通道
// 每个 Channel 对应一个 Topic，Consumer 在首次调用 OnReceive 时懒创建
//
//nolint:copylocks // Channel is a protobuf message type alias, passing by value is required by interface
func (t *rocketmqTransport) Channel(ch Channel) (TransportChannel, error) {
	return &rocketmqChannel{transport: t, channel: ch}, nil
}

func (t *rocketmqTransport) Close() error {
	if t.consumers != nil {
		t.mu.RLock()
		for _, c := range t.consumers {
			if c != nil {
				_ = c.GracefulStop()
			}
		}
		t.mu.RUnlock()
	}

	if t.producer != nil {
		_ = t.producer.GracefulStop()
	}
	return nil
}

func (t *rocketmqTransport) getOrCreateConsumer(topic string, filter *golang.FilterExpression) (golang.SimpleConsumer, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.consumers[topic]; ok {
		return c, nil
	}

	rmqCfg := &golang.Config{
		Endpoint:      t.config.Endpoint,
		NameSpace:     t.config.Namespace,
		ConsumerGroup: t.config.GroupID,
	}

	if t.config.AccessKey != "" && t.config.SecretKey != "" {
		rmqCfg.Credentials = &credentials.SessionCredentials{
			AccessKey:    t.config.AccessKey,
			AccessSecret: t.config.SecretKey,
		}
	}

	opts := []golang.SimpleConsumerOption{
		golang.WithSubscriptionExpressions(map[string]*golang.FilterExpression{topic: filter}),
	}

	c, err := golang.NewSimpleConsumer(rmqCfg, opts...)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}

	if err := c.Start(); err != nil {
		return nil, fmt.Errorf("start consumer: %w", err)
	}

	t.consumers[topic] = c
	return c, nil
}

func (c *rocketmqChannel) Send(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error {
	topic := c.channel.Address
	if topic == "" {
		return errEmptyTopic
	}

	binding := parseBinding(c.channel.Binding)
	msg := buildMessage(topic, event, binding)

	switch binding.msgType {
	case eventapiv1.OperationRule_Channel_Binding_Rocketmq_DELAY:
		return sendDelay(ctx, c.transport.producer, msg, binding.delayTimestamp)
	case eventapiv1.OperationRule_Channel_Binding_Rocketmq_ORDER:
		return sendOrder(ctx, c.transport.producer, msg, event)
	case eventapiv1.OperationRule_Channel_Binding_Rocketmq_TRANSACTION:
		return sendNormal(ctx, c.transport.producer, msg, c.transport.config.Async)
	default:
		return sendNormal(ctx, c.transport.producer, msg, c.transport.config.Async)
	}
}

// SendTransaction 发送事务消息，callback 为本地事务执行函数
func (c *rocketmqChannel) SendTransaction(ctx context.Context, event *cloudevent.CloudEvent[[]byte], callback func(context.Context) error) error {
	topic := c.channel.Address
	if topic == "" {
		return errEmptyTopic
	}

	binding := parseBinding(c.channel.Binding)
	msg := buildMessage(topic, event, binding)
	return sendTransaction(ctx, c.transport.producer, msg, callback)
}

type bindingConfig struct {
	tag            string
	key            string
	msgType        eventapiv1.OperationRule_Channel_Binding_Rocketmq_MessageType
	delayTimestamp *time.Time
}

func parseBinding(binding *eventapiv1.OperationRule_Channel_Binding) bindingConfig {
	cfg := bindingConfig{msgType: eventapiv1.OperationRule_Channel_Binding_Rocketmq_NORMAL}

	if binding == nil || binding.Rocketmq == nil {
		return cfg
	}

	rmq := binding.Rocketmq
	return bindingConfig{
		tag:            rmq.Tag,
		key:            rmq.Key,
		msgType:        rmq.MessageType,
		delayTimestamp: nil,
	}
}

func buildMessage(topic string, event *cloudevent.CloudEvent[[]byte], binding bindingConfig) *golang.Message {
	msg := &golang.Message{Topic: topic, Body: event.Data}

	if binding.tag != "" {
		msg.SetTag(binding.tag)
	}
	if binding.key != "" {
		msg.SetKeys(binding.key)
	}

	if props := msg.GetProperties(); props != nil {
		for k, v := range buildProperties(event) {
			props[k] = v
		}
	}

	return msg
}

func buildProperties(event *cloudevent.CloudEvent[[]byte]) map[string]string {
	props := map[string]string{
		"ce-specversion": event.SpecVersion,
		"ce-type":        event.Type,
		"ce-source":      event.Source,
		"ce-id":          event.Id,
	}

	if event.Subject != "" {
		props[cePrefix+"subject"] = event.Subject
	}
	if event.DataContentType != "" {
		props["content-type"] = event.DataContentType
	}
	if event.DataSchema != "" {
		props[cePrefix+"dataschema"] = event.DataSchema
	}
	if !event.Time.IsZero() {
		props[cePrefix+"time"] = event.Time.Format(time.RFC3339)
	}

	if event.Extensions != nil {
		for k, v := range event.Extensions {
			if k == "delay_timestamp" || k == "delay_level" {
				continue
			}
			if data, err := json.Marshal(v); err == nil {
				props[cePrefix+k] = string(data)
			}
		}
	}

	return props
}

func sendDelay(ctx context.Context, p golang.Producer, msg *golang.Message, ts *time.Time) error {
	if ts != nil {
		msg.SetDelayTimestamp(*ts)
	}
	_, err := p.Send(ctx, msg)
	return err
}

func sendOrder(ctx context.Context, p golang.Producer, msg *golang.Message, event *cloudevent.CloudEvent[[]byte]) error {
	shardingKey := event.Subject
	if shardingKey == "" {
		shardingKey = event.Id
	}
	msg.SetMessageGroup(shardingKey)
	_, err := p.Send(ctx, msg)
	return err
}

func sendTransaction(ctx context.Context, p golang.Producer, msg *golang.Message, callback func(context.Context) error) error {
	tx := p.BeginTransaction()
	if _, err := p.SendWithTransaction(ctx, msg, tx); err != nil {
		return fmt.Errorf("send half message: %w", err)
	}

	defer func() {
		if r := recover(); r != nil {
			tx.RollBack()
			panic(r)
		}
	}()

	if err := callback(ctx); err != nil {
		if rbErr := tx.RollBack(); rbErr != nil {
			return fmt.Errorf("local transaction failed: %w, rollback error: %v", err, rbErr)
		}
		return fmt.Errorf("local transaction failed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func sendNormal(ctx context.Context, p golang.Producer, msg *golang.Message, async bool) error {
	if async {
		p.SendAsync(ctx, msg, func(_ context.Context, _ []*golang.SendReceipt, _ error) {})
		return nil
	}
	_, err := p.Send(ctx, msg)
	return err
}

func (c *rocketmqChannel) OnReceive(ctx context.Context, handle func(context.Context, *ReceivedEvent[[]byte]) error) error {
	topic := c.channel.Address
	if topic == "" {
		return errEmptyTopic
	}

	filter := golang.NewFilterExpression("*")
	if binding := c.channel.Binding; binding != nil && binding.Rocketmq != nil {
		if tag := binding.Rocketmq.Tag; tag != "" {
			filter = golang.NewFilterExpression(tag)
		}
	}

	consumer, err := c.transport.getOrCreateConsumer(topic, filter)
	if err != nil {
		return err
	}

	go c.receiveLoop(ctx, consumer, handle)
	return nil
}

func (c *rocketmqChannel) receiveLoop(ctx context.Context, consumer golang.SimpleConsumer, handle func(context.Context, *ReceivedEvent[[]byte]) error) {
	batchSize := int32(defaultIf(c.transport.config.BatchSize, defaultBatchSize))
	invisibleDuration := c.transport.config.InvisibleDuration
	if invisibleDuration <= 0 {
		invisibleDuration = defaultInvisibleDuration
	}

	baseDelay := 100 * time.Millisecond
	maxDelay := 30 * time.Second
	retries := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, err := consumer.Receive(ctx, batchSize, invisibleDuration)
		if err != nil {
			delay := baseDelay * time.Duration(1<<retries)
			if delay > maxDelay {
				delay = maxDelay
			}
			time.Sleep(delay)
			retries++
			continue
		}
		retries = 0

		// RocketMQ 拉取模式: 批量获取消息后逐条处理
		for _, mv := range msgs {
			event := toCloudEvent(mv)
			msgID := mv.GetMessageId()
			reconsumeTimes := mv.GetDeliveryAttempt()

			received := &ReceivedEvent[[]byte]{
				Event: event,
				Ack: func(ctx context.Context) error {
					return consumer.Ack(ctx, mv)
				},
				MessageID:      msgID,
				ReconsumeTimes: reconsumeTimes,
			}

			// panic recover 包裹 handler
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[eventapi][rocketmq] handler panic: %v", r)
					}
				}()
				if err := handle(ctx, received); err != nil {
					log.Printf("[eventapi][rocketmq] handler error: %v", err)
				}
			}()
		}
	}
}

func toCloudEvent(mv *golang.MessageView) *cloudevent.CloudEvent[[]byte] {
	event := &cloudevent.CloudEvent[[]byte]{Data: mv.GetBody()}
	props := mv.GetProperties()

	mapper := map[string]*string{
		"ce-specversion": &event.SpecVersion,
		"ce-type":        &event.Type,
		"ce-source":      &event.Source,
		"ce-id":          &event.Id,
		"ce-subject":     &event.Subject,
		"content-type":   &event.DataContentType,
		"ce-dataschema":  &event.DataSchema,
	}

	for k, v := range props {
		if dst, ok := mapper[k]; ok && *dst == "" {
			*dst = v
		}
	}

	if event.Id == "" {
		event.Id = mv.GetMessageId()
	}

	if t, err := time.Parse(time.RFC3339, props["ce-time"]); err == nil {
		event.Time = t
	}

	event.Extensions = make(cloudevent.Extensions)
	for k, v := range props {
		if len(k) > 3 && k[:3] == cePrefix {
			var val any
			if json.Unmarshal([]byte(v), &val) == nil {
				event.Extensions[k[3:]] = cloudevent.NewAttributeValue(val)
			}
		}
	}

	if event.SpecVersion == "" {
		event.SpecVersion = "1.0"
	}
	return event
}

func defaultIf(val, def int) int {
	if val <= 0 {
		return def
	}
	return val
}
