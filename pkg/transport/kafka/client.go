// Package kafka Kafka 传输层实现
//
// # 设计思想
//
// Kafka 使用基于偏移量的消息确认机制。消费者通过 CommitMessages() 提交已处理消息的偏移量，
// 未提交的消息在消费者重启后会重新投递。
//
// # 架构
//
// kafkaTransport 管理全局 Writer，负责消息发送。
// kafkaChannel 对应单个 Topic，每个 Channel 独立创建 Reader 进行消费。
//
// # 接收模型
//
// OnReceive 使用 sync.Once 确保每个 Channel 只启动一个 receiveLoop goroutine。
// receiveLoop 内部使用指数退避重试 FetchMessage，避免网络抖动导致消费中断。
// 每条消息处理时包裹 panic recover，防止单个 handler panic 导致整个消费循环退出。
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	eventapiv1 "github.com/eventapi/eventapi-go/pkg/eventapi/v1"
	kafkago "github.com/segmentio/kafka-go"
)

// Config Kafka 传输层配置。
//
// 使用示例:
//
//	// 基本配置（仅发送）
//	cfg := kafka.Config{
//	    Brokers: []string{"localhost:9092"},
//	}
//
//	// 发送 + 接收
//	cfg := kafka.Config{
//	    Brokers: []string{"localhost:9092"},
//	    GroupID: "my-consumer-group",
//	}
//
//	// 高性能批量发送
//	cfg := kafka.Config{
//	    Brokers:      []string{"localhost:9092"},
//	    Async:        true,
//	    BatchSize:    500,
//	    BatchBytes:   5 * 1024 * 1024, // 5MB
//	    BatchTimeout: 50 * time.Millisecond,
//	}
type Config struct {
	// Brokers Kafka broker 地址列表，必填。
	// 示例: []string{"localhost:9092", "localhost:9093"}
	Brokers []string

	// ClientID 客户端标识，用于 broker 端识别和监控。
	ClientID string

	// Timeout 写入超时时间。默认 30s。
	Timeout time.Duration

	// Async 是否启用异步发送。
	// true: 消息写入内部队列后立即返回，不等待 broker 确认（性能高，可能丢消息）
	// false: 等待 broker 确认写入后返回（可靠性高）
	Async bool

	// BatchSize 批量发送的消息数量。默认 100。
	// 仅在 Async=true 时生效。
	BatchSize int

	// BatchBytes 批量发送的最大字节数。默认 1MB。
	// 仅在 Async=true 时生效。
	BatchBytes int

	// BatchTimeout 刷新批量消息前的最大等待时间。
	// 即使未达到 BatchSize/BatchBytes，超时后也会立即发送。默认 0（不等待）。
	BatchTimeout time.Duration

	// GroupID 消费者组 ID。
	// 仅接收时需要配置；仅发送时可为空。
	// 同一 GroupID 的消费者实例会共享消费进度。
	GroupID string
}

// Channel 是 IDL 生成的 Channel 类型别名
type Channel = eventapiv1.OperationRule_Channel

// kafkaTransport Kafka 传输层实现
type kafkaTransport struct {
	config Config
	writer *kafkago.Writer
}

// kafkaChannel Kafka 通道实现
type kafkaChannel struct {
	transport   *kafkaTransport
	channel     Channel
	reader      *kafkago.Reader
	receiveOnce sync.Once
	receiveErr  error
	cancel      context.CancelFunc
	closed      chan struct{}
}

// New 创建 Kafka Transport。
//
// 默认值:
//   - BatchSize: 100
//   - BatchBytes: 1MB
//   - Timeout: 30s
func New(_ context.Context, cfg Config) (Transport, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("brokers cannot be empty")
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	batchBytes := cfg.BatchBytes
	if batchBytes <= 0 {
		batchBytes = 1048576
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Balancer:     &kafkago.LeastBytes{},
		BatchSize:    batchSize,
		BatchBytes:   int64(batchBytes),
		BatchTimeout: cfg.BatchTimeout,
		Async:        cfg.Async,
		WriteTimeout: timeout,
	}

	return &kafkaTransport{
		config: cfg,
		writer: writer,
	}, nil
}

// Channel 返回指定通道的 TransportChannel
// 每个 Channel 对应一个独立的 Topic，Reader 在首次调用 OnReceive 时延迟创建
func (t *kafkaTransport) Channel(ch Channel) (TransportChannel, error) {
	return &kafkaChannel{
		transport: t,
		channel:   ch,
	}, nil
}

// Close 关闭 Transport
func (t *kafkaTransport) Close() error {
	if t.writer != nil {
		t.writer.Close()
	}
	return nil
}

func cloudEventToKafkaHeaders(event *cloudevent.CloudEvent[[]byte]) []kafkago.Header {
	headers := []kafkago.Header{
		{Key: "ce-specversion", Value: []byte(event.SpecVersion)},
		{Key: "ce-type", Value: []byte(event.Type)},
		{Key: "ce-source", Value: []byte(event.Source)},
		{Key: "ce-id", Value: []byte(event.Id)},
	}

	if event.Subject != "" {
		headers = append(headers, kafkago.Header{Key: "ce-subject", Value: []byte(event.Subject)})
	}

	if event.DataContentType != "" {
		headers = append(headers, kafkago.Header{Key: "content-type", Value: []byte(event.DataContentType)})
	}

	if event.DataSchema != "" {
		headers = append(headers, kafkago.Header{Key: "ce-dataschema", Value: []byte(event.DataSchema)})
	}

	if !event.Time.IsZero() {
		headers = append(headers, kafkago.Header{Key: "ce-time", Value: []byte(event.Time.Format(time.RFC3339))})
	}

	for k, v := range event.Extensions {
		if data, err := json.Marshal(v); err == nil {
			headers = append(headers, kafkago.Header{Key: "ce-" + k, Value: data})
		}
	}

	return headers
}

func kafkaHeadersToCloudEvent(headers []kafkago.Header, data []byte) *cloudevent.CloudEvent[[]byte] {
	event := &cloudevent.CloudEvent[[]byte]{
		Data: data,
	}

	for _, h := range headers {
		switch h.Key {
		case "ce-specversion":
			event.SpecVersion = string(h.Value)
		case "ce-type":
			event.Type = string(h.Value)
		case "ce-source":
			event.Source = string(h.Value)
		case "ce-id":
			event.Id = string(h.Value)
		case "ce-subject":
			event.Subject = string(h.Value)
		case "content-type":
			event.DataContentType = string(h.Value)
		case "ce-dataschema":
			event.DataSchema = string(h.Value)
		case "ce-time":
			if t, err := time.Parse(time.RFC3339, string(h.Value)); err == nil {
				event.Time = t
			}
		default:
			if len(h.Key) > 3 && h.Key[:3] == "ce-" {
				if event.Extensions == nil {
					event.Extensions = make(cloudevent.Extensions)
				}
				var val any
				if json.Unmarshal(h.Value, &val) == nil {
					event.Extensions[h.Key[3:]] = cloudevent.NewAttributeValue(val)
				}
			}
		}
	}

	if event.SpecVersion == "" {
		event.SpecVersion = "1.0"
	}

	return event
}

func (c *kafkaChannel) Send(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error {
	headers := cloudEventToKafkaHeaders(event)

	kafkaMsg := kafkago.Message{
		Topic:   c.channel.Address,
		Key:     []byte(event.Subject),
		Value:   event.Data,
		Headers: headers,
	}

	return c.transport.writer.WriteMessages(ctx, kafkaMsg)
}

// OnReceive 注册接收处理器
// 使用 sync.Once 确保每个 channel 只启动一次消费
func (c *kafkaChannel) OnReceive(ctx context.Context, handle func(context.Context, *ReceivedEvent[[]byte]) error) error {
	if c.transport.config.GroupID == "" {
		return fmt.Errorf("GroupID is required for receiving messages")
	}

	c.receiveOnce.Do(func() {
		c.receiveErr = c.startReceiving(ctx, handle)
	})

	return c.receiveErr
}

// startReceiving 启动消息接收循环
func (c *kafkaChannel) startReceiving(ctx context.Context, handle func(context.Context, *ReceivedEvent[[]byte]) error) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.closed = make(chan struct{})

	// 创建 Reader
	c.reader = kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  c.transport.config.Brokers,
		Topic:    c.channel.Address,
		GroupID:  c.transport.config.GroupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	go c.receiveLoop(ctx, handle)
	return nil
}

// receiveLoop 消息接收循环
func (c *kafkaChannel) receiveLoop(ctx context.Context, handle func(context.Context, *ReceivedEvent[[]byte]) error) error {
	defer func() {
		if c.reader != nil {
			c.reader.Close()
		}
		close(c.closed)
	}()

	maxRetries := 5
	baseDelay := 100 * time.Millisecond
	maxDelay := 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// 带重试的消息获取
		var kafkaMsg kafkago.Message
		var err error
		var retries int

		for retries = 0; retries < maxRetries; retries++ {
			kafkaMsg, err = c.reader.FetchMessage(ctx)
			if err == nil {
				break
			}

			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil
			}

			// 指数退避
			delay := baseDelay * time.Duration(1<<retries)
			if delay > maxDelay {
				delay = maxDelay
			}
			time.Sleep(delay)
		}

		if err != nil {
			continue
		}

		event := kafkaHeadersToCloudEvent(kafkaMsg.Headers, kafkaMsg.Value)

		reader := c.reader
		msg := kafkaMsg
		// 闭包捕获 reader 和 msg，确保 Commit 操作针对当前消息
		commitFunc := func(ctx context.Context) error {
			return reader.CommitMessages(ctx, msg)
		}

		receivedEvent := &ReceivedEvent[[]byte]{
			Event:     event,
			Commit:    commitFunc,
			Partition: int32(kafkaMsg.Partition),
			Offset:    kafkaMsg.Offset,
		}

		// panic recover 包裹 handler，防止单个 handler panic 导致整个消费循环退出
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[eventapi][kafka] handler panic: %v", r)
				}
			}()
			if err := handle(ctx, receivedEvent); err != nil {
				log.Printf("[eventapi][kafka] handler error: %v", err)
			}
		}()
	}
}
