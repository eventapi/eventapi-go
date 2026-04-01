// Package rocketmq RocketMQ 传输层实现
//
// 支持延时消息、事务消息、顺序消息
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
	"github.com/eventapi/eventapi-go/pkg/transport"
)

const (
	cePrefix = "ce-"

	defaultMaxRetry          = 3
	defaultBatchSize         = 1
	defaultInvisibleDuration = 30 * time.Second
)

var errEmptyTopic = errors.New("channel address (topic) is empty")
var errEmptyEndpoint = errors.New("endpoint is required")

type Config struct {
	Endpoint           string
	GroupID            string
	AccessKey          string
	SecretKey          string
	Namespace          string
	SendTimeout        time.Duration
	MaxRetry           int
	Async              bool
	BatchSize          int
	AwaitDuration      time.Duration
	InvisibleDuration  time.Duration
	TransactionChecker *TransactionChecker
}

type TransactionChecker struct {
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

func New(ctx context.Context, cfg Config) (transport.Transport[Channel], error) {
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

// Channel creates a new rocketmq channel for the given configuration
//
//nolint:copylocks // Channel is a protobuf message type alias, passing by value is required by interface
func (t *rocketmqTransport) Channel(ch Channel) (transport.TransportChannel, error) {
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

func (c *rocketmqChannel) OnReceive(ctx context.Context, handle transport.OnReceiveHandle[[]byte]) error {
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

func (c *rocketmqChannel) receiveLoop(ctx context.Context, consumer golang.SimpleConsumer, handle transport.OnReceiveHandle[[]byte]) {
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

		for _, mv := range msgs {
			event := toCloudEvent(mv)
			received := &transport.ReceivedEvent[[]byte]{
				Event: event,
				Ack: func(ctx context.Context) error {
					return consumer.Ack(ctx, mv)
				},
			}

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
