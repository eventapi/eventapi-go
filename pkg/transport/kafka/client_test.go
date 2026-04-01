package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/eventapi/eventapi-go/pkg/cloudevent"
	"github.com/eventapi/eventapi-go/pkg/transport"
	kafkago "github.com/segmentio/kafka-go"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: Config{
				Brokers: []string{"localhost:9092"},
			},
			wantErr: false,
		},
		{
			name:    "empty brokers",
			config:  Config{},
			wantErr: true,
			errMsg:  "brokers cannot be empty",
		},
		{
			name: "default values",
			config: Config{
				Brokers: []string{"localhost:9092"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, err := New(context.Background(), tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("New() expected error, got nil")
					return
				}
				if err.Error() != tt.errMsg {
					t.Errorf("New() error = %v, want %v", err.Error(), tt.errMsg)
				}
				return
			}
			if err != nil {
				t.Errorf("New() unexpected error: %v", err)
				return
			}
			if tr == nil {
				t.Error("New() returned nil transport")
			}
			tr.Close()
		})
	}
}

func TestChannel(t *testing.T) {
	tr, err := New(context.Background(), Config{
		Brokers: []string{"localhost:9092"},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer tr.Close()

	ch := Channel{
		Address: "test.topic",
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

func TestCloudEventToKafkaHeaders(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	event := &cloudevent.CloudEvent[[]byte]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id:              "test-id",
			Source:          "urn:test:source",
			SpecVersion:     "1.0",
			Type:            "test.event.v1",
			DataContentType: "application/json",
			DataSchema:      "https://example.com/schema",
			Time:            now,
			Subject:         "user.123",
			Extensions: cloudevent.Extensions{}.
				WithString("trace_id", "abc-123").
				WithInt64("timestamp", 1234567890),
		},
		Data: []byte(`{"key":"value"}`),
	}

	headers := cloudEventToKafkaHeaders(event)

	headerMap := make(map[string]string)
	for _, h := range headers {
		headerMap[h.Key] = string(h.Value)
	}

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
		{"ce-time", now.Format(time.RFC3339)},
	}

	for _, tt := range tests {
		if got := headerMap[tt.key]; got != tt.want {
			t.Errorf("header[%s] = %s, want %s", tt.key, got, tt.want)
		}
	}

	if val, ok := headerMap["ce-trace_id"]; !ok {
		t.Error("missing ce-trace_id header")
	} else {
		var s string
		if err := json.Unmarshal([]byte(val), &s); err != nil {
			t.Errorf("failed to unmarshal trace_id: %v", err)
		}
		if s != "abc-123" {
			t.Errorf("ce-trace_id = %s, want abc-123", s)
		}
	}
}

func TestKafkaHeadersToCloudEvent(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	traceIdJSON, _ := json.Marshal("abc-123")

	headers := []kafkago.Header{
		{Key: "ce-specversion", Value: []byte("1.0")},
		{Key: "ce-type", Value: []byte("test.event.v1")},
		{Key: "ce-source", Value: []byte("urn:test:source")},
		{Key: "ce-id", Value: []byte("test-id")},
		{Key: "ce-subject", Value: []byte("user.123")},
		{Key: "content-type", Value: []byte("application/json")},
		{Key: "ce-dataschema", Value: []byte("https://example.com/schema")},
		{Key: "ce-time", Value: []byte(now.Format(time.RFC3339))},
		{Key: "ce-trace_id", Value: traceIdJSON},
	}

	data := []byte(`{"key":"value"}`)
	event := kafkaHeadersToCloudEvent(headers, data)

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
	if !event.Time.Equal(now) {
		t.Errorf("Time = %v, want %v", event.Time, now)
	}

	if val, ok := event.Extensions.Get("trace_id"); !ok {
		t.Error("missing trace_id extension")
	} else {
		s, ok := val.AsString()
		if !ok {
			t.Error("trace_id is not string")
		}
		if s != "abc-123" {
			t.Errorf("trace_id = %s, want abc-123", s)
		}
	}
}

func TestCloudEventRoundTrip(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	original := &cloudevent.CloudEvent[[]byte]{
		CloudEventMetadata: cloudevent.CloudEventMetadata{
			Id:              "test-id",
			Source:          "urn:test:source",
			SpecVersion:     "1.0",
			Type:            "test.event.v1",
			DataContentType: "application/json",
			Subject:         "user.123",
			Time:            now,
			Extensions: cloudevent.Extensions{}.
				WithString("trace_id", "abc-123"),
		},
		Data: []byte(`{"key":"value"}`),
	}

	headers := cloudEventToKafkaHeaders(original)
	restored := kafkaHeadersToCloudEvent(headers, original.Data)

	if original.Id != restored.Id {
		t.Errorf("Id mismatch: %s != %s", original.Id, restored.Id)
	}
	if original.Type != restored.Type {
		t.Errorf("Type mismatch: %s != %s", original.Type, restored.Type)
	}
	if original.Source != restored.Source {
		t.Errorf("Source mismatch: %s != %s", original.Source, restored.Source)
	}
	if original.Subject != restored.Subject {
		t.Errorf("Subject mismatch: %s != %s", original.Subject, restored.Subject)
	}
}

func TestOnReceiveWithoutGroupID(t *testing.T) {
	tr, err := New(context.Background(), Config{
		Brokers: []string{"localhost:9092"},
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	defer tr.Close()

	ch, err := tr.Channel(Channel{Address: "test.topic"})
	if err != nil {
		t.Fatalf("Channel() error: %v", err)
	}

	err = ch.OnReceive(context.Background(), func(ctx context.Context, event *transport.ReceivedEvent[[]byte]) error {
		return nil
	})

	if err == nil {
		t.Error("OnReceive() without GroupID should return error")
	}
}
