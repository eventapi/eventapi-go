// Package kafka Kafka 传输层实现
//
// Kafka 使用基于偏移量的消息确认机制
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
	"github.com/eventapi/eventapi-go/pkg/transport"
	kafkago "github.com/segmentio/kafka-go"
)

// Config Kafka 传输层配置
type Config struct {
	// Brokers Kafka broker 地址列表
	Brokers []string

	// ClientID 客户端标识
	ClientID string

	// Timeout 连接超时时间
	Timeout time.Duration

	// Async 是否启用异步发送
	Async bool

	// BatchSize 批量发送的消息数量
	BatchSize int

	// BatchBytes 批量大小的最大字节数
	BatchBytes int

	// BatchTimeout 刷新批量消息前的等待时间
	BatchTimeout time.Duration

	// GroupID 消费者组 ID
	// 如果为空且调用 OnReceive，会返回错误
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

// New 创建 Kafka Transport
func New(_ context.Context, cfg Config) (transport.Transport[Channel], error) {
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
func (t *kafkaTransport) Channel(ch Channel) (transport.TransportChannel, error) {
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
func (c *kafkaChannel) OnReceive(ctx context.Context, handle transport.OnReceiveHandle[[]byte]) error {
	if c.transport.config.GroupID == "" {
		return fmt.Errorf("GroupID is required for receiving messages")
	}

	c.receiveOnce.Do(func() {
		c.receiveErr = c.startReceiving(ctx, handle)
	})

	return c.receiveErr
}

// startReceiving 启动消息接收循环
func (c *kafkaChannel) startReceiving(ctx context.Context, handle transport.OnReceiveHandle[[]byte]) error {
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
func (c *kafkaChannel) receiveLoop(ctx context.Context, handle transport.OnReceiveHandle[[]byte]) {
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
			return
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
				return
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
		ackFunc := func(ctx context.Context) error {
			return reader.CommitMessages(ctx, msg)
		}

		receivedEvent := &transport.ReceivedEvent[[]byte]{
			Event: event,
			Ack:   ackFunc,
		}

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
