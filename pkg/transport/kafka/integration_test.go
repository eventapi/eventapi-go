//go:build integration

package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	"github.com/eventapi/eventapi-go/pkg/transport"
	kafkago "github.com/segmentio/kafka-go"
)

var kafkaBroker string

func TestMain(m *testing.M) {
	kafkaBroker = os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}
	os.Exit(m.Run())
}

func createTopic(t *testing.T, topic string) {
	t.Helper()

	conn, err := kafkago.DialLeader(context.Background(), "tcp", kafkaBroker, topic, 0)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("failed to get controller: %v", err)
	}

	ctrlConn, err := kafkago.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		t.Fatalf("failed to dial controller: %v", err)
	}
	defer ctrlConn.Close()

	err = ctrlConn.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil && err.Error() != "[36] Topic Already Exists: topic already exists" {
		t.Logf("create topic warning: %v", err)
	}
}

func TestSendAndReceive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "test.send.receive"
	createTopic(t, topic)

	tr, err := New(ctx, Config{
		Brokers: []string{kafkaBroker},
		GroupID: "test-group",
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer tr.Close()

	ch, err := tr.Channel(Channel{Address: topic})
	if err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	event := &cloudevent.CloudEvent[[]byte]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id:              "test-id-1",
			Source:          "urn:test:source",
			SpecVersion:     "1.0",
			Type:            "test.event.v1",
			DataContentType: "application/json",
			Subject:         "user.123",
			Extensions: cloudevent.Extensions{}.
				WithString("trace_id", "abc-123"),
		},
		Data: []byte(`{"key":"value"}`),
	}

	err = ch.Send(ctx, event)
	if err != nil {
		t.Fatalf("Send() error: %v", err)
	}

	done := make(chan struct{})
	err = ch.OnReceive(ctx, func(ctx context.Context, re *transport.ReceivedEvent[[]byte]) error {
		defer close(done)

		if re.Event.Type != "test.event.v1" {
			t.Errorf("Type = %s, want test.event.v1", re.Event.Type)
		}
		if re.Event.Subject != "user.123" {
			t.Errorf("Subject = %s, want user.123", re.Event.Subject)
		}
		if string(re.Event.Data) != `{"key":"value"}` {
			t.Errorf("Data = %s, want {\"key\":\"value\"}", string(re.Event.Data))
		}

		val, ok := re.Event.Extensions.Get("trace_id")
		if !ok {
			t.Error("missing trace_id extension")
		} else {
			s, _ := val.AsString()
			if s != "abc-123" {
				t.Errorf("trace_id = %s, want abc-123", s)
			}
		}

		return re.Ack(ctx)
	})
	if err != nil {
		t.Fatalf("OnReceive() error: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestSendMultipleMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "test.multiple.messages"
	createTopic(t, topic)

	tr, err := New(ctx, Config{
		Brokers: []string{kafkaBroker},
		GroupID: "test-group-multi",
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer tr.Close()

	ch, err := tr.Channel(Channel{Address: topic})
	if err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	messages := []string{"msg1", "msg2", "msg3"}
	for _, msg := range messages {
		event := &cloudevent.CloudEvent[[]byte]{
			CloudEventMetadata: cloudevent.CloudEventMetadata{
				Id:          msg,
				Source:      "urn:test:source",
				SpecVersion: "1.0",
				Type:        "test.multi.v1",
			},
			Data: []byte(msg),
		}
		if err := ch.Send(ctx, event); err != nil {
			t.Fatalf("Send() error: %v", err)
		}
	}

	received := make(map[string]bool)
	done := make(chan struct{})

	err = ch.OnReceive(ctx, func(ctx context.Context, re *transport.ReceivedEvent[[]byte]) error {
		received[re.Event.Id] = true
		re.Ack(ctx)

		if len(received) == len(messages) {
			close(done)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("OnReceive() error: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for messages")
	}

	for _, msg := range messages {
		if !received[msg] {
			t.Errorf("message %s not received", msg)
		}
	}
}

func TestAsyncSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "test.async.send"
	createTopic(t, topic)

	tr, err := New(ctx, Config{
		Brokers:   []string{kafkaBroker},
		Async:     true,
		BatchSize: 2,
		GroupID:   "test-group-async",
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer tr.Close()

	ch, err := tr.Channel(Channel{Address: topic})
	if err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	for i := 0; i < 5; i++ {
		event := &cloudevent.CloudEvent[[]byte]{
			CloudEventMetadata: cloudevent.CloudEventMetadata{
				Id:          fmt.Sprintf("async-%d", i),
				Source:      "urn:test:source",
				SpecVersion: "1.0",
				Type:        "test.async.v1",
			},
			Data: []byte(fmt.Sprintf("message-%d", i)),
		}
		if err := ch.Send(ctx, event); err != nil {
			t.Fatalf("Send() error: %v", err)
		}
	}

	time.Sleep(2 * time.Second)

	received := 0
	done := make(chan struct{})

	err = ch.OnReceive(ctx, func(ctx context.Context, re *transport.ReceivedEvent[[]byte]) error {
		received++
		re.Ack(ctx)

		if received >= 5 {
			close(done)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("OnReceive() error: %v", err)
	}

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("timeout, received %d/5 messages", received)
	}
}
