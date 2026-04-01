package rocketmq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	"github.com/eventapi/eventapi-go/pkg/codec"
	eventapiv1 "github.com/eventapi/eventapi-go/pkg/eventapi/v1"
)

func TestNewWithEmptyEndpoint(t *testing.T) {
	_, err := New(context.Background(), Config{})
	if err == nil {
		t.Error("New() with empty endpoint should return error")
	}
	if err != nil && err.Error() != "endpoint is required" {
		t.Errorf("New() error = %v, want endpoint is required", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := Config{
		Endpoint: "localhost:8080",
		GroupID:  "test-group",
	}

	if cfg.SendTimeout != 0 {
		t.Logf("SendTimeout default: %v", cfg.SendTimeout)
	}
	if cfg.MaxRetry != 0 {
		t.Logf("MaxRetry default: %v", cfg.MaxRetry)
	}
	if cfg.BatchSize != 0 {
		t.Logf("BatchSize default: %v", cfg.BatchSize)
	}
}

func TestSendWithEmptyTopic(t *testing.T) {
	tr, err := New(context.Background(), Config{
		Endpoint: "localhost:8080",
		GroupID:  "test-group",
	})
	if err != nil {
		t.Skipf("Cannot create transport: %v", err)
	}

	ch, err := tr.Channel(Channel{Address: ""})
	if err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	event := &cloudevent.CloudEvent[[]byte]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id:      "test-id",
			Source:  "test-source",
			Type:    "test.event",
			Subject: "test-subject",
		},
		Data: []byte("test data"),
	}

	err = ch.Send(context.Background(), event)
	if err == nil {
		t.Error("Send() with empty topic should return error")
	}
}

func TestOnReceiveWithEmptyTopic(t *testing.T) {
	tr, err := New(context.Background(), Config{
		Endpoint: "localhost:8080",
		GroupID:  "test-group",
	})
	if err != nil {
		t.Skipf("Cannot create transport: %v", err)
	}

	ch, err := tr.Channel(Channel{Address: ""})
	if err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	err = ch.(*rocketmqChannel).OnReceive(context.Background(), func(ctx context.Context, event *ReceivedEvent[[]byte]) error {
		return nil
	})
	if err == nil {
		t.Error("OnReceive() with empty topic should return error")
	}
}

func TestChannelWithBinding(t *testing.T) {
	tr, err := New(context.Background(), Config{
		Endpoint: "localhost:8080",
		GroupID:  "test-group",
	})
	if err != nil {
		t.Skipf("Cannot create transport: %v", err)
	}

	ch := Channel{
		Address: "test.topic",
		Binding: &eventapiv1.OperationRule_Channel_Binding{
			Rocketmq: &eventapiv1.OperationRule_Channel_Binding_Rocketmq{
				Tag:         "TEST_TAG",
				Key:         "test-key",
				MessageType: eventapiv1.OperationRule_Channel_Binding_Rocketmq_NORMAL,
			},
		},
	}
	channel, err := tr.Channel(ch)
	if err != nil {
		t.Errorf("Channel() error: %v", err)
		return
	}
	if channel == nil {
		t.Error("Channel() returned nil")
	}
}

func BenchmarkCloudEventConversion(b *testing.B) {
	event := &cloudevent.CloudEvent[[]byte]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id:              "test-id",
			Source:          "urn:test:source",
			SpecVersion:     "1.0",
			Type:            "test.event.v1",
			DataContentType: "application/json",
			Subject:         "user.123",
		},
		Data: []byte(`{"key":"value"}`),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = event.Id
		_ = event.Type
		_ = event.Source
		_ = event.Subject
		_ = event.SpecVersion
		_ = event.DataContentType
		_ = event.DataSchema
		_ = event.Time
		_ = event.Data
		_ = event.Extensions
	}
}

func TestEventBuilderSend(t *testing.T) {
	var sentEvent *cloudevent.CloudEvent[[]byte]
	sendFn := func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error {
		sentEvent = event
		return nil
	}

	ce := &cloudevent.CloudEvent[*TestData]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id:          "test-id",
			Source:      "urn:test",
			SpecVersion: "1.0",
			Type:        "test.event",
		},
		Data: &TestData{Name: "test"},
	}

	err := NewEventBuilder(ce, codec.NewJSON(), sendFn, nil).Send(context.Background())
	if err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	if sentEvent == nil {
		t.Fatal("Send() did not call sendFn")
	}
	if string(sentEvent.Data) != `{"name":"test"}` {
		t.Errorf("Send() data = %s, want {\"name\":\"test\"}", string(sentEvent.Data))
	}
}

func TestEventBuilderWithDelay(t *testing.T) {
	var sentEvent *cloudevent.CloudEvent[[]byte]
	sendFn := func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error {
		sentEvent = event
		return nil
	}

	ce := &cloudevent.CloudEvent[*TestData]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id: "test-id", Source: "urn:test", SpecVersion: "1.0", Type: "test.event",
		},
		Data: &TestData{Name: "test"},
	}

	err := NewEventBuilder(ce, codec.NewJSON(), sendFn, nil).
		WithDelayAt(now).
		Send(context.Background())
	if err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	if _, ok := sentEvent.Extensions.Get("delay_timestamp"); !ok {
		t.Error("WithDelayAt() did not set delay_timestamp extension")
	}
}

func TestEventBuilderWithMessageGroup(t *testing.T) {
	var sentEvent *cloudevent.CloudEvent[[]byte]
	sendFn := func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error {
		sentEvent = event
		return nil
	}

	ce := &cloudevent.CloudEvent[*TestData]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id: "test-id", Source: "urn:test", SpecVersion: "1.0", Type: "test.event",
		},
		Data: &TestData{Name: "test"},
	}

	err := NewEventBuilder(ce, codec.NewJSON(), sendFn, nil).
		WithMessageGroup("order-123").
		Send(context.Background())
	if err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	if v, ok := sentEvent.Extensions.Get("message_group"); !ok {
		t.Error("WithMessageGroup() did not set message_group extension")
	} else if s, _ := v.AsString(); s != "order-123" {
		t.Errorf("message_group = %s, want order-123", s)
	}
}

func TestEventBuilderTransactionCallbackSuccess(t *testing.T) {
	var txSendCalled bool
	var txCallbackCalled bool
	txSendFn := func(ctx context.Context, event *cloudevent.CloudEvent[[]byte], callback func(context.Context) error) error {
		txSendCalled = true
		return callback(ctx)
	}
	sendFn := func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error {
		t.Fatal("sendFn should not be called for transaction")
		return nil
	}

	ce := &cloudevent.CloudEvent[*TestData]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id: "test-id", Source: "urn:test", SpecVersion: "1.0", Type: "test.event",
		},
		Data: &TestData{Name: "test"},
	}

	err := NewEventBuilder(ce, codec.NewJSON(), sendFn, txSendFn).
		WithTransaction(func(ctx context.Context) error {
			txCallbackCalled = true
			return nil
		}).
		Send(context.Background())
	if err != nil {
		t.Fatalf("Send() error: %v", err)
	}
	if !txSendCalled {
		t.Error("transactionSend should be called")
	}
	if !txCallbackCalled {
		t.Error("transaction callback should be called")
	}
}

func TestEventBuilderTransactionCallbackError(t *testing.T) {
	expectedErr := errors.New("local tx failed")
	txSendFn := func(ctx context.Context, event *cloudevent.CloudEvent[[]byte], callback func(context.Context) error) error {
		return callback(ctx)
	}
	sendFn := func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error {
		t.Fatal("sendFn should not be called for transaction")
		return nil
	}

	ce := &cloudevent.CloudEvent[*TestData]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id: "test-id", Source: "urn:test", SpecVersion: "1.0", Type: "test.event",
		},
		Data: &TestData{Name: "test"},
	}

	err := NewEventBuilder(ce, codec.NewJSON(), sendFn, txSendFn).
		WithTransaction(func(ctx context.Context) error {
			return expectedErr
		}).
		Send(context.Background())
	if err == nil {
		t.Fatal("Send() should return error when callback fails")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("Send() error = %v, want %v", err, expectedErr)
	}
}

func TestEventBuilderNonTransactionPath(t *testing.T) {
	var sendCalled int32
	sendFn := func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error {
		atomic.StoreInt32(&sendCalled, 1)
		return nil
	}

	ce := &cloudevent.CloudEvent[*TestData]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id: "test-id", Source: "urn:test", SpecVersion: "1.0", Type: "test.event",
		},
		Data: &TestData{Name: "test"},
	}

	_ = NewEventBuilder(ce, codec.NewJSON(), sendFn, nil).Send(context.Background())
	if atomic.LoadInt32(&sendCalled) != 1 {
		t.Error("sendFn should be called for non-transaction path")
	}
}

func TestParseBinding(t *testing.T) {
	tests := []struct {
		name    string
		binding *eventapiv1.OperationRule_Channel_Binding
		want    bindingConfig
	}{
		{
			name:    "nil binding",
			binding: nil,
			want:    bindingConfig{msgType: eventapiv1.OperationRule_Channel_Binding_Rocketmq_NORMAL},
		},
		{
			name:    "normal message",
			binding: &eventapiv1.OperationRule_Channel_Binding{Rocketmq: &eventapiv1.OperationRule_Channel_Binding_Rocketmq{MessageType: eventapiv1.OperationRule_Channel_Binding_Rocketmq_NORMAL}},
			want:    bindingConfig{msgType: eventapiv1.OperationRule_Channel_Binding_Rocketmq_NORMAL},
		},
		{
			name:    "with tag and key",
			binding: &eventapiv1.OperationRule_Channel_Binding{Rocketmq: &eventapiv1.OperationRule_Channel_Binding_Rocketmq{Tag: "TAG1", Key: "key1"}},
			want:    bindingConfig{tag: "TAG1", key: "key1", msgType: eventapiv1.OperationRule_Channel_Binding_Rocketmq_NORMAL},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseBinding(tt.binding)
			if got.tag != tt.want.tag {
				t.Errorf("tag = %s, want %s", got.tag, tt.want.tag)
			}
			if got.key != tt.want.key {
				t.Errorf("key = %s, want %s", got.key, tt.want.key)
			}
			if got.msgType != tt.want.msgType {
				t.Errorf("msgType = %v, want %v", got.msgType, tt.want.msgType)
			}
		})
	}
}

func TestBuildProperties(t *testing.T) {
	event := &cloudevent.CloudEvent[[]byte]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id:              "test-id",
			Source:          "urn:test",
			SpecVersion:     "1.0",
			Type:            "test.event",
			Subject:         "user.123",
			DataContentType: "application/json",
		},
		Data: []byte(`{}`),
	}

	props := buildProperties(event)

	if props["ce-specversion"] != "1.0" {
		t.Errorf("ce-specversion = %s, want 1.0", props["ce-specversion"])
	}
	if props["ce-type"] != "test.event" {
		t.Errorf("ce-type = %s, want test.event", props["ce-type"])
	}
	if props["ce-source"] != "urn:test" {
		t.Errorf("ce-source = %s, want urn:test", props["ce-source"])
	}
	if props["ce-id"] != "test-id" {
		t.Errorf("ce-id = %s, want test-id", props["ce-id"])
	}
	if props["ce-subject"] != "user.123" {
		t.Errorf("ce-subject = %s, want user.123", props["ce-subject"])
	}
}

func TestDefaultIf(t *testing.T) {
	tests := []struct {
		val  int
		def  int
		want int
	}{
		{0, 10, 10},
		{-1, 10, 10},
		{5, 10, 5},
		{100, 10, 100},
	}

	for _, tt := range tests {
		if got := defaultIf(tt.val, tt.def); got != tt.want {
			t.Errorf("defaultIf(%d, %d) = %d, want %d", tt.val, tt.def, got, tt.want)
		}
	}
}

type TestData struct {
	Name string `json:"name"`
}

var now = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
