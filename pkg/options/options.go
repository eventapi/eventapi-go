package options

import (
	"context"

	"github.com/eventapi/eventapi-go/pkg/codec"
	"github.com/eventapi/eventapi-go/pkg/transport"
	"github.com/eventapi/eventapi-go/pkg/uuid"
)

// TransportConfig 是所有传输层配置的接口标记
// 实现此接口的类型包括:
// - kafkatransport.Config
// - amqptransport.Config
// 等其他消息队列配置
type TransportConfig interface{}

// OptionFn 是函数式选项类型
// 泛型参数 T 必须是实现了 TransportConfig 的类型
type OptionFn[T TransportConfig, C transport.Channel] func(*Options[T, C])

// Options 保存客户端配置的完整状态
// T 是具体的传输层配置类型 (如 kafka.Config)
type Options[T TransportConfig, C transport.Channel] struct {
	// TransportBuilder 用于创建 Transport 的工厂函数
	// 如果为 nil，则使用 NewKafkaTransport 作为默认实现
	TransportBuilder func(context.Context, T) (transport.Transport[C], error)

	// Codec 用于事件的序列化和反序列化
	// 如果为 nil，默认使用 JSONCodec
	Codec codec.Codec

	// UUIDGenerator 用于生成 UUID
	// 如果为 nil，默认使用 uuid.Generate
	UUIDGenerator func() string

	// Source 是 CloudEvent 的事件来源标识
	// 格式: URI 或 URN
	// 如果为空，默认使用 "urn:eventapi:client"
	Source string

	// SpecVersion 是 CloudEvents 规范版本
	// 如果为空，默认使用 "1.0"
	SpecVersion string
}

// WithTransportBuilder 创建用于设置 TransportBuilder 的选项
// 使用示例:
//
//	opts := []options.OptionFn[kafkatransport.Config, kafkatransport.Channel]{
//	    options.WithTransportBuilder(func(ctx context.Context, cfg kafkatransport.Config) (transport.Transport[kafkatransport.Channel], error) {
//	        return kafkatransport.New(cfg)
//	    }),
//	}
func WithTransportBuilder[T TransportConfig, C transport.Channel](
	builder func(context.Context, T) (transport.Transport[C], error),
) OptionFn[T, C] {
	return func(o *Options[T, C]) {
		o.TransportBuilder = builder
	}
}

// WithCodec 创建用于设置 Codec 的选项
// 使用示例:
//
//	opts := []options.OptionFn[kafkatransport.Config, kafkatransport.Channel]{
//	    options.WithCodec(codec.NewJSON()),
//	}
func WithCodec[T TransportConfig, C transport.Channel](c codec.Codec) OptionFn[T, C] {
	return func(o *Options[T, C]) {
		o.Codec = c
	}
}

// WithUUIDGenerator 创建用于设置 UUID 生成器的选项
func WithUUIDGenerator[T TransportConfig, C transport.Channel](fn func() string) OptionFn[T, C] {
	return func(o *Options[T, C]) {
		o.UUIDGenerator = fn
	}
}

// WithSource 创建用于设置 CloudEvent Source 的选项
func WithSource[T TransportConfig, C transport.Channel](source string) OptionFn[T, C] {
	return func(o *Options[T, C]) {
		o.Source = source
	}
}

// DefaultOptions 返回默认配置
func DefaultOptions[T TransportConfig, C transport.Channel]() *Options[T, C] {
	return &Options[T, C]{
		Codec:         codec.NewJSON(),
		UUIDGenerator: uuid.Generate,
		Source:        "urn:eventapi:client",
		SpecVersion:   "1.0",
	}
}

// NewUUID 返回一个新的 UUID 字符串
func (o *Options[T, C]) NewUUID() string {
	if o.UUIDGenerator != nil {
		return o.UUIDGenerator()
	}
	return uuid.Generate()
}

// GetSource 返回 Source，如果为空则返回默认值
func (o *Options[T, C]) GetSource() string {
	if o.Source != "" {
		return o.Source
	}
	return "urn:eventapi:client"
}

// GetSpecVersion 返回 SpecVersion，如果为空则返回默认值
func (o *Options[T, C]) GetSpecVersion() string {
	if o.SpecVersion != "" {
		return o.SpecVersion
	}
	return "1.0"
}
