// Package amqp AMQP 传输层实现
//
// 支持消息优先级、交换机、路由键等特性
package amqp

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	eventapiv1 "github.com/eventapi/eventapi-go/pkg/eventapi/v1"
	"github.com/eventapi/eventapi-go/pkg/transport"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Config AMQP 传输层配置
type Config struct {
	// URL AMQP 连接地址
	URL string

	// Username 用户名
	Username string

	// Password 密码
	Password string

	// Vhost 虚拟主机
	Vhost string

	// ChannelMax 最大通道数
	ChannelMax int

	// FrameMax 最大帧大小
	FrameMax int

	// Heartbeat 心跳间隔
	Heartbeat time.Duration

	// GroupID 消费者组 ID
	GroupID string

	// PrefetchCount 消费者预取数量，0表示使用服务器默认值
	PrefetchCount int

	// PoolSize 发送 channel 池大小，0表示不启用池
	PoolSize int
}

// Channel 是 IDL 生成的 Channel 类型别名
type Channel = eventapiv1.OperationRule_Channel

// amqpTransport AMQP 传输层实现
type amqpTransport struct {
	config    Config
	conn      *amqp091.Connection
	channels  map[string]*amqpChannel
	mu        sync.RWMutex
	sendCh    chan *amqp091.Channel
	closing   atomic.Bool
	closeOnce sync.Once
}

// amqpChannel AMQP 通道实现
type amqpChannel struct {
	transport   *amqpTransport
	channel     Channel
	consumerTag string
	deliveries  <-chan amqp091.Delivery
	once        sync.Once
	err         error
}

// New 创建 AMQP Transport
func New(_ context.Context, cfg Config) (transport.Transport[Channel], error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("AMQP URL is required")
	}

	conn, err := amqp091.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to AMQP: %w", err)
	}

	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = 1
	}

	t := &amqpTransport{
		config:   cfg,
		conn:     conn,
		channels: make(map[string]*amqpChannel),
		sendCh:   make(chan *amqp091.Channel, poolSize),
	}

	for i := 0; i < poolSize; i++ {
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to create channel pool: %w", err)
		}
		t.sendCh <- ch
	}

	return t, nil
}

// Channel 返回指定通道的 TransportChannel
func (t *amqpTransport) Channel(ch Channel) (transport.TransportChannel, error) {
	t.mu.RLock()
	if existing, ok := t.channels[ch.Address]; ok {
		t.mu.RUnlock()
		return existing, nil
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	if existing, ok := t.channels[ch.Address]; ok {
		return existing, nil
	}

	chObj := &amqpChannel{
		transport: t,
		channel:   ch,
	}
	t.channels[ch.Address] = chObj
	return chObj, nil
}

// Close 关闭 Transport
func (t *amqpTransport) Close() error {
	var closeErr error
	t.closeOnce.Do(func() {
		t.closing.Store(true)

		// Drain and close all channels in pool
		if t.sendCh != nil {
			close(t.sendCh)
			for ch := range t.sendCh {
				if ch != nil && !ch.IsClosed() {
					ch.Close()
				}
			}
		}

		if t.conn != nil {
			closeErr = t.conn.Close()
		}
	})
	return closeErr
}

// getChannel 从 pool 获取 channel，若失效则重建
func (t *amqpTransport) getChannel() (*amqp091.Channel, error) {
	select {
	case ch, ok := <-t.sendCh:
		if !ok {
			return nil, fmt.Errorf("transport is closed")
		}
		if ch.IsClosed() {
			return t.conn.Channel()
		}
		return ch, nil
	default:
		if t.conn == nil {
			return nil, fmt.Errorf("connection is nil")
		}
		return t.conn.Channel()
	}
}

// returnChannel 归还 channel 到 pool
func (t *amqpTransport) returnChannel(ch *amqp091.Channel) {
	if ch == nil || ch.IsClosed() {
		return
	}
	if t.closing.Load() {
		ch.Close()
		return
	}
	select {
	case t.sendCh <- ch:
	default:
		// pool 已满，关闭多余 channel
		ch.Close()
	}
}

func cloudEventToAMQPHeaders(event *cloudevent.CloudEvent[[]byte]) amqp091.Table {
	headers := amqp091.Table{
		"ce-specversion": event.SpecVersion,
		"ce-type":        event.Type,
		"ce-source":      event.Source,
		"ce-id":          event.Id,
	}

	if event.Subject != "" {
		headers["ce-subject"] = event.Subject
	}

	if event.DataContentType != "" {
		headers["content-type"] = event.DataContentType
	}

	if event.DataSchema != "" {
		headers["ce-dataschema"] = event.DataSchema
	}

	return headers
}

func (c *amqpChannel) Send(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error {
	if c.transport.closing.Load() {
		return fmt.Errorf("transport is closing")
	}

	ch, err := c.transport.getChannel()
	if err != nil {
		return fmt.Errorf("get channel: %w", err)
	}
	defer func() { c.transport.returnChannel(ch) }()

	headers := cloudEventToAMQPHeaders(event)

	queueName := c.channel.Address
	routingKey := queueName

	if c.channel.Binding != nil && c.channel.Binding.Amqp != nil {
		binding := c.channel.Binding.Amqp

		if binding.Exchange != "" {
			err = ch.ExchangeDeclare(
				binding.Exchange,
				"direct",
				true,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				return fmt.Errorf("failed to declare exchange: %w", err)
			}

			_, err = ch.QueueDeclare(
				queueName,
				true,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				return fmt.Errorf("failed to declare queue: %w", err)
			}

			err = ch.QueueBind(
				queueName,
				routingKey,
				binding.Exchange,
				false,
				nil,
			)
			if err != nil {
				return fmt.Errorf("failed to bind queue: %w", err)
			}
		}

		if binding.RoutingKey != "" {
			routingKey = binding.RoutingKey
		}
	}

	return ch.PublishWithContext(
		ctx,
		"",
		routingKey,
		false,
		false,
		amqp091.Publishing{
			ContentType:  event.DataContentType,
			Body:         event.Data,
			Headers:      headers,
			DeliveryMode: amqp091.Persistent,
		},
	)
}

// OnReceive 注册接收处理器
func (c *amqpChannel) OnReceive(ctx context.Context, handle transport.OnReceiveHandle[[]byte]) error {
	c.once.Do(func() {
		c.err = c.startReceiving(ctx, handle)
	})
	return c.err
}

func (c *amqpChannel) startReceiving(ctx context.Context, handle transport.OnReceiveHandle[[]byte]) error {
	ch, err := c.transport.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}

	prefetch := c.transport.config.PrefetchCount
	if prefetch > 0 {
		err = ch.Qos(prefetch, 0, false)
		if err != nil {
			return fmt.Errorf("failed to set QoS: %w", err)
		}
	}

	queueName := c.channel.Address

	if c.channel.Binding != nil && c.channel.Binding.Amqp != nil {
		binding := c.channel.Binding.Amqp

		if binding.Exchange != "" {
			err = ch.ExchangeDeclare(
				binding.Exchange,
				"direct",
				true,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				return fmt.Errorf("failed to declare exchange: %w", err)
			}

			_, err = ch.QueueDeclare(
				queueName,
				true,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				return fmt.Errorf("failed to declare queue: %w", err)
			}

			err = ch.QueueBind(
				queueName,
				binding.RoutingKey,
				binding.Exchange,
				false,
				nil,
			)
			if err != nil {
				return fmt.Errorf("failed to bind queue: %w", err)
			}
		}
	}

	deliveries, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	c.deliveries = deliveries

	go c.receiveLoop(ctx, handle)

	return nil
}

func (c *amqpChannel) receiveLoop(ctx context.Context, handle transport.OnReceiveHandle[[]byte]) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-c.deliveries:
			if !ok {
				return
			}

			event := amqpDeliveryToCloudEvent(d)

			ackFunc := func(ctx context.Context) error {
				return d.Ack(false)
			}

			wrappedEvent := transport.ReceivedEvent[[]byte]{
				Event: event,
				Ack:   ackFunc,
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[eventapi][amqp] handler panic: %v", r)
					}
				}()
				if err := handle(ctx, &wrappedEvent); err != nil {
					log.Printf("[eventapi][amqp] handler error: %v", err)
					_ = d.Nack(false, true)
				}
			}()
		}
	}
}

func amqpDeliveryToCloudEvent(d amqp091.Delivery) *cloudevent.CloudEvent[[]byte] {
	event := &cloudevent.CloudEvent[[]byte]{
		Data: d.Body,
	}

	if d.ContentType != "" {
		event.DataContentType = d.ContentType
	}

	headers := amqp091.Table(d.Headers)
	if v, ok := headers["ce-specversion"].(string); ok {
		event.SpecVersion = v
	}
	if v, ok := headers["ce-type"].(string); ok {
		event.Type = v
	}
	if v, ok := headers["ce-source"].(string); ok {
		event.Source = v
	}
	if v, ok := headers["ce-id"].(string); ok {
		event.Id = v
	}
	if v, ok := headers["ce-subject"].(string); ok {
		event.Subject = v
	}
	if v, ok := headers["ce-dataschema"].(string); ok {
		event.DataSchema = v
	}

	if event.SpecVersion == "" {
		event.SpecVersion = "1.0"
	}

	return event
}
