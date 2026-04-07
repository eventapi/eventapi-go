// Package amqp AMQP 传输层实现
//
// # 设计思想
//
// AMQP 使用显式确认机制 (basic.ack / basic.nack)。消费者处理完消息后必须调用 Ack() 确认，
// 否则消息会在消费者断开连接后重新投递。Nack(requeue=true) 可将消息重新入队。
//
// # 架构
//
// amqpTransport 管理单一 TCP 连接和发送 Channel 池。
// 发送时从池中获取 Channel，用完后归还，避免频繁创建销毁。
// 每个 amqpChannel 对应一个 Queue，接收时创建独立的 AMQP Channel 进行消费。
//
// # 接收模型
//
// OnReceive 使用 sync.Once 确保每个 Channel 只启动一个 receiveLoop goroutine。
// receiveLoop 监听 deliveries channel，收到消息后构建 ReceivedEvent 并调用 handler。
// handler 返回错误时自动 Nack(requeue=true) 重新入队。
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
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Config AMQP 传输层配置。
//
// 使用示例:
//
//	// 基本配置
//	cfg := amqp.Config{
//	    URL: "amqp://guest:guest@localhost:5672/",
//	}
//
//	// 带认证和预取控制
//	cfg := amqp.Config{
//	    URL:           "amqp://localhost:5672/",
//	    Username:      "myuser",
//	    Password:      "mypassword",
//	    Vhost:         "/production",
//	    PrefetchCount: 10,  // 每次预取 10 条消息
//	    PoolSize:      5,   // 发送 Channel 池大小
//	}
type Config struct {
	// URL AMQP 连接地址，必填。
	// 格式: amqp://[username:password@]host[:port][/vhost]
	// 示例: "amqp://guest:guest@localhost:5672/"
	URL string

	// Username 用户名。如果 URL 中已包含认证信息，可留空。
	Username string

	// Password 密码。如果 URL 中已包含认证信息，可留空。
	Password string

	// Vhost 虚拟主机。如果 URL 中已指定，可留空。
	Vhost string

	// ChannelMax 单个连接允许的最大通道数。默认 0（不限制）。
	ChannelMax int

	// FrameMax 最大帧大小（字节）。默认 0（使用服务器协商值）。
	FrameMax int

	// Heartbeat 心跳间隔。默认 0（不发送心跳）。
	// 建议设置 10-30s 以检测连接断开。
	Heartbeat time.Duration

	// GroupID 消费者组标识。
	// 仅接收时建议使用；仅发送时可为空。
	GroupID string

	// PrefetchCount 消费者预取数量。
	// 控制未确认消息的最大数量，0 表示使用服务器默认值。
	// 值越小，消费越慢但越均匀；值越大，吞吐越高但可能积压在单个消费者。
	PrefetchCount int

	// PoolSize 发送 Channel 池大小。默认 1。
	// Channel 从池中获取，用完后归还，避免频繁创建销毁。
	// 设为 0 时退化为每次发送创建新 Channel。
	PoolSize int

	// Enabled 是否启用此传输通道。
	// false 时 Send() 静默跳过，OnReceive() 不启动消费者。
	// 注意: Go bool 零值为 false，需显式设为 true 启用
	Enabled bool
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
	channel     *eventapiv1.OperationRule_Channel
	consumerTag string
	deliveries  <-chan amqp091.Delivery
	once        sync.Once
	err         error
}

// New 创建 AMQP Transport。
//
// 建立 TCP 连接并创建发送 Channel 池。
// 默认池大小为 1。
func New(_ context.Context, cfg Config) (Transport, error) {
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
// 使用读写锁实现 Channel 缓存，避免重复创建
func (t *amqpTransport) Channel(ch Channel) (TransportChannel, error) {
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
		channel:   &ch,
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
	if !c.transport.config.Enabled {
		return nil
	}
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
func (c *amqpChannel) OnReceive(ctx context.Context, handle func(context.Context, *ReceivedEvent[[]byte]) error) error {
	if !c.transport.config.Enabled {
		return nil
	}
	c.once.Do(func() {
		c.err = c.startReceiving(ctx, handle)
	})
	return c.err
}

func (c *amqpChannel) startReceiving(ctx context.Context, handle func(context.Context, *ReceivedEvent[[]byte]) error) error {
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

func (c *amqpChannel) receiveLoop(ctx context.Context, handle func(context.Context, *ReceivedEvent[[]byte]) error) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-c.deliveries:
			if !ok {
				return
			}

			event := amqpDeliveryToCloudEvent(d)

			// 闭包捕获 delivery，确保 Ack/Nack 操作针对当前投递
			ackFunc := func(ctx context.Context) error {
				return d.Ack(false)
			}

			nackFunc := func(ctx context.Context, requeue bool) error {
				return d.Nack(false, requeue)
			}

			wrappedEvent := ReceivedEvent[[]byte]{
				Event: event,
				Ack:   ackFunc,
				Nack:  nackFunc,
			}

			// panic recover 包裹 handler，handler 返回错误时自动 Nack(requeue=true) 重新入队
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
