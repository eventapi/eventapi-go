// Package middleware 提供 Kafka 传输层中间件。
package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/eventapi/eventapi-go/pkg/transport/kafka"
	kafkago "github.com/segmentio/kafka-go"
)

// SendLogger 打印发送消息的 Topic、Headers、Body 和耗时。
func SendLogger(level slog.Level) kafka.WriterMiddleware {
	return func(next kafka.WriteFunc) kafka.WriteFunc {
		return func(ctx context.Context, msg *kafkago.Message) error {
			start := time.Now()
			err := next(ctx, msg)
			attrs := []slog.Attr{
				slog.String("topic", msg.Topic),
				slog.Duration("latency", time.Since(start)),
				slog.Any("error", err),
			}
			for _, h := range msg.Headers {
				attrs = append(attrs, slog.String(h.Key, string(h.Value)))
			}
			attrs = append(attrs, slog.String("body", string(msg.Value)))
			slog.LogAttrs(ctx, level, "Kafka Send", attrs...)
			return err
		}
	}
}

// ReceiveLogger 打印接收消息的 Topic、Headers、Body、Offset 和耗时。
func ReceiveLogger(level slog.Level) kafka.ReaderMiddleware {
	return func(next kafka.FetchFunc) kafka.FetchFunc {
		return func(ctx context.Context) (*kafkago.Message, error) {
			start := time.Now()
			msg, err := next(ctx)
			if err != nil {
				return nil, err
			}
			attrs := []slog.Attr{
				slog.String("topic", msg.Topic),
				slog.Int("partition", int(msg.Partition)),
				slog.Int64("offset", msg.Offset),
				slog.Duration("latency", time.Since(start)),
			}
			for _, h := range msg.Headers {
				attrs = append(attrs, slog.String(h.Key, string(h.Value)))
			}
			attrs = append(attrs, slog.String("body", string(msg.Value)))
			slog.LogAttrs(ctx, level, "Kafka Receive", attrs...)
			return msg, nil
		}
	}
}

// SendRetry 发送失败时重试。
func SendRetry(maxRetries int, backoff time.Duration) kafka.WriterMiddleware {
	return func(next kafka.WriteFunc) kafka.WriteFunc {
		return func(ctx context.Context, msg *kafkago.Message) error {
			var lastErr error
			for i := 0; i <= maxRetries; i++ {
				lastErr = next(ctx, msg)
				if lastErr == nil {
					return nil
				}
				if i < maxRetries {
					slog.WarnContext(ctx, "Kafka Send retry",
						slog.String("topic", msg.Topic),
						slog.Int("attempt", i+1),
						slog.Any("error", lastErr),
					)
					time.Sleep(backoff * time.Duration(i+1))
				}
			}
			return lastErr
		}
	}
}
