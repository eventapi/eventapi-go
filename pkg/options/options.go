package options

import (
	"context"

	"github.com/eventapi/eventapi-go/pkg/codec"
	"github.com/eventapi/eventapi-go/pkg/uuid"
)

// OptionFn 是函数式选项类型
type OptionFn[T any, TR any] func(*Options[T, TR])

// Options 保存客户端配置的完整状态
// T 是具体的传输层配置类型 (如 kafka.Config)
// TR 是传输层接口 (如 kafka.Transport)
type Options[T any, TR any] struct {
	// TransportBuilder 用于创建 Transport 的工厂函数
	TransportBuilder func(context.Context, T) (TR, error)

	// Codec 用于事件的序列化和反序列化
	Codec codec.Codec

	// UUIDGenerator 用于生成 UUID
	UUIDGenerator func() string

	// Source 是 CloudEvent 的事件来源标识
	Source string

	// SpecVersion 是 CloudEvents 规范版本
	SpecVersion string

	// MetadataExtractor 元数据提取器
	MetadataExtractor func(ctx context.Context) map[string]string
}

// WithTransportBuilder 创建用于设置 TransportBuilder 的选项
func WithTransportBuilder[T any, TR any](
	builder func(context.Context, T) (TR, error),
) OptionFn[T, TR] {
	return func(o *Options[T, TR]) {
		o.TransportBuilder = builder
	}
}

// WithCodec 创建用于设置 Codec 的选项
func WithCodec[T any, TR any](c codec.Codec) OptionFn[T, TR] {
	return func(o *Options[T, TR]) {
		o.Codec = c
	}
}

// WithUUIDGenerator 创建用于设置 UUID 生成器的选项
func WithUUIDGenerator[T any, TR any](fn func() string) OptionFn[T, TR] {
	return func(o *Options[T, TR]) {
		o.UUIDGenerator = fn
	}
}

// WithSource 创建用于设置 CloudEvent Source 的选项
func WithSource[T any, TR any](source string) OptionFn[T, TR] {
	return func(o *Options[T, TR]) {
		o.Source = source
	}
}

// DefaultOptions 返回默认配置
func DefaultOptions[T any, TR any]() *Options[T, TR] {
	return &Options[T, TR]{
		Codec:         codec.NewJSON(),
		UUIDGenerator: uuid.Generate,
		Source:        "urn:eventapi:client",
		SpecVersion:   "1.0",
	}
}

// NewUUID 返回一个新的 UUID 字符串
func (o *Options[T, TR]) NewUUID() string {
	if o.UUIDGenerator != nil {
		return o.UUIDGenerator()
	}
	return uuid.Generate()
}

// GetSource 返回 Source，如果为空则返回默认值
func (o *Options[T, TR]) GetSource() string {
	if o.Source != "" {
		return o.Source
	}
	return "urn:eventapi:client"
}

// GetSpecVersion 返回 SpecVersion，如果为空则返回默认值
func (o *Options[T, TR]) GetSpecVersion() string {
	if o.SpecVersion != "" {
		return o.SpecVersion
	}
	return "1.0"
}
