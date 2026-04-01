package amqp

import (
	"context"
	"sync"
	"testing"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	"github.com/eventapi/eventapi-go/pkg/codec"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

func TestNewWithEmptyURL(t *testing.T) {
	_, err := New(context.Background(), Config{})
	if err == nil {
		t.Error("New() with empty URL should return error")
	}
	if err == nil || err.Error() != "AMQP URL is required" {
		t.Errorf("New() error = %v, want 'AMQP URL is required'", err)
	}
}

func TestNewWithInvalidURL(t *testing.T) {
	_, err := New(context.Background(), Config{URL: "not-a-valid-url"})
	if err == nil {
		t.Error("New() with invalid URL should return error")
	}
}

func TestChannelCaching(t *testing.T) {
	tr := &amqpTransport{
		channels: make(map[string]*amqpChannel),
		sendCh:   make(chan *amqp091.Channel, 1),
	}

	ch1 := Channel{Address: "test.queue"}
	c1, err := tr.Channel(ch1)
	if err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	c2, err := tr.Channel(ch1)
	if err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	if c1 != c2 {
		t.Error("Channel() should return cached instance for same address")
	}
}

func TestGetChannelFromPool(t *testing.T) {
	tr := &amqpTransport{
		sendCh: make(chan *amqp091.Channel, 1),
	}

	mockCh := &amqp091.Channel{}
	tr.sendCh <- mockCh

	ch, err := tr.getChannel()
	if err != nil {
		t.Fatalf("getChannel() error: %v", err)
	}
	if ch != mockCh {
		t.Error("getChannel() should return channel from pool")
	}
}

func TestGetChannelWhenPoolEmpty(t *testing.T) {
	tr := &amqpTransport{
		sendCh: make(chan *amqp091.Channel, 1),
	}

	_, err := tr.getChannel()
	if err == nil {
		t.Error("getChannel() with nil conn should return error")
	}
}

func TestGetChannelWhenClosed(t *testing.T) {
	tr := &amqpTransport{
		sendCh: make(chan *amqp091.Channel, 1),
	}
	close(tr.sendCh)

	_, err := tr.getChannel()
	if err == nil {
		t.Error("getChannel() should return error when transport is closed")
	}
}

func TestReturnChannel(t *testing.T) {
	tr := &amqpTransport{
		sendCh: make(chan *amqp091.Channel, 1),
	}

	mockCh := &amqp091.Channel{}
	tr.returnChannel(mockCh)

	select {
	case ch := <-tr.sendCh:
		if ch != mockCh {
			t.Error("returnChannel() should put channel back to pool")
		}
	default:
		t.Error("returnChannel() did not return channel to pool")
	}
}

func TestReturnChannelWhenPoolFull(t *testing.T) {
	t.Skip("cannot test with fake amqp091.Channel — IsClosed() panics without real connection")
}

func TestReturnChannelWhenClosing(t *testing.T) {
	t.Skip("cannot test with fake amqp091.Channel — IsClosed() panics without real connection")
}

func TestReturnChannelNil(t *testing.T) {
	tr := &amqpTransport{
		sendCh: make(chan *amqp091.Channel, 1),
	}
	tr.returnChannel(nil)
}

func TestCloseIdempotent(t *testing.T) {
	tr := &amqpTransport{
		sendCh: make(chan *amqp091.Channel, 1),
	}

	err1 := tr.Close()
	err2 := tr.Close()

	if err1 != err2 {
		t.Errorf("Close() should be idempotent: err1=%v, err2=%v", err1, err2)
	}
}

func TestCloseSetsClosingFlag(t *testing.T) {
	tr := &amqpTransport{
		sendCh: make(chan *amqp091.Channel, 1),
	}

	tr.Close()

	if !tr.closing.Load() {
		t.Error("Close() should set closing flag")
	}
}

func TestSendWhenClosing(t *testing.T) {
	tr := &amqpTransport{
		sendCh: make(chan *amqp091.Channel, 1),
	}
	tr.closing.Store(true)

	c := &amqpChannel{transport: tr}
	err := c.Send(context.Background(), &cloudevent.CloudEvent[[]byte]{})
	if err == nil {
		t.Error("Send() should return error when transport is closing")
	}
}

func TestCloudEventToAMQPHeaders(t *testing.T) {
	event := &cloudevent.CloudEvent[[]byte]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id:              "test-id",
			Source:          "urn:test:source",
			SpecVersion:     "1.0",
			Type:            "test.event.v1",
			DataContentType: "application/json",
			DataSchema:      "https://example.com/schema",
			Subject:         "user.123",
		},
		Data: []byte(`{"key":"value"}`),
	}

	headers := cloudEventToAMQPHeaders(event)

	tests := []struct {
		key  string
		want string
	}{
		{"ce-specversion", "1.0"},
		{"ce-type", "test.event.v1"},
		{"ce-source", "urn:test:source"},
		{"ce-id", "test-id"},
		{"ce-subject", "user.123"},
		{"content-type", "application/json"},
		{"ce-dataschema", "https://example.com/schema"},
	}

	for _, tt := range tests {
		if got, ok := headers[tt.key].(string); !ok || got != tt.want {
			t.Errorf("headers[%s] = %v, want %s", tt.key, headers[tt.key], tt.want)
		}
	}
}

func TestAMQPDeliveryToCloudEvent(t *testing.T) {
	d := amqp091.Delivery{
		Body:        []byte(`{"key":"value"}`),
		ContentType: "application/json",
		Headers: amqp091.Table{
			"ce-specversion": "1.0",
			"ce-type":        "test.event.v1",
			"ce-source":      "urn:test:source",
			"ce-id":          "test-id",
			"ce-subject":     "user.123",
			"ce-dataschema":  "https://example.com/schema",
		},
	}

	event := amqpDeliveryToCloudEvent(d)

	if event.SpecVersion != "1.0" {
		t.Errorf("SpecVersion = %s, want 1.0", event.SpecVersion)
	}
	if event.Type != "test.event.v1" {
		t.Errorf("Type = %s, want test.event.v1", event.Type)
	}
	if event.Source != "urn:test:source" {
		t.Errorf("Source = %s, want urn:test:source", event.Source)
	}
	if event.Id != "test-id" {
		t.Errorf("Id = %s, want test-id", event.Id)
	}
	if event.Subject != "user.123" {
		t.Errorf("Subject = %s, want user.123", event.Subject)
	}
	if event.DataContentType != "application/json" {
		t.Errorf("DataContentType = %s, want application/json", event.DataContentType)
	}
	if event.DataSchema != "https://example.com/schema" {
		t.Errorf("DataSchema = %s, want https://example.com/schema", event.DataSchema)
	}
	if string(event.Data) != `{"key":"value"}` {
		t.Errorf("Data = %s, want {\"key\":\"value\"}", string(event.Data))
	}
}

func TestAMQPDeliveryToCloudEventDefaults(t *testing.T) {
	d := amqp091.Delivery{
		Body: []byte(`{}`),
	}

	event := amqpDeliveryToCloudEvent(d)

	if event.SpecVersion != "1.0" {
		t.Errorf("SpecVersion default = %s, want 1.0", event.SpecVersion)
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

	err := NewEventBuilder(ce, codec.NewJSON(), sendFn).Send(context.Background())
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

func TestEventBuilderWithExtensions(t *testing.T) {
	sendFn := func(ctx context.Context, event *cloudevent.CloudEvent[[]byte]) error { return nil }

	ce := &cloudevent.CloudEvent[*TestData]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id: "test-id", Source: "urn:test", SpecVersion: "1.0", Type: "test.event",
		},
		Data: &TestData{Name: "test"},
	}

	ext := make(cloudevent.Extensions)
	ext.WithString("trace_id", "abc")
	NewEventBuilder(ce, codec.NewJSON(), sendFn).
		WithExtensions(ext).
		WithSubject("user.123")

	if _, ok := ext.Get("trace_id"); !ok {
		t.Error("WithExtensions() did not set extension")
	}
	if ce.Subject != "user.123" {
		t.Errorf("WithSubject() did not set subject: %s", ce.Subject)
	}
}

func TestEventBuilderWithExchange(t *testing.T) {
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

	err := NewEventBuilder(ce, codec.NewJSON(), sendFn).
		WithExchange("my.exchange").
		WithRoutingKey("my.key").
		WithPriority(5).
		Send(context.Background())
	if err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	if v, ok := sentEvent.Extensions.Get("amqp_exchange"); !ok {
		t.Error("WithExchange() did not set extension")
	} else if s, _ := v.AsString(); s != "my.exchange" {
		t.Errorf("amqp_exchange = %s, want my.exchange", s)
	}

	if v, ok := sentEvent.Extensions.Get("amqp_routing_key"); !ok {
		t.Error("WithRoutingKey() did not set extension")
	} else if s, _ := v.AsString(); s != "my.key" {
		t.Errorf("amqp_routing_key = %s, want my.key", s)
	}

	if _, ok := sentEvent.Extensions.Get("amqp_priority"); !ok {
		t.Error("WithPriority() did not set extension")
	}
}

func TestOnReceiveWithoutConn(t *testing.T) {
	t.Skip("cannot test with fake amqp091.Connection — Channel() panics without real connection")
}

func TestConcurrentClose(t *testing.T) {
	tr := &amqpTransport{
		sendCh: make(chan *amqp091.Channel, 1),
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr.Close()
		}()
	}
	wg.Wait()
}

type TestData struct {
	Name string `json:"name"`
}
