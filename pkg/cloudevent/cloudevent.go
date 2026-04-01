package cloudevent

import (
	"encoding/json"
	"time"
)

// CloudEvent 表示 CloudEvents 1.0 规范的事件
// T 是事件数据的类型，可以是具体的业务结构体或 []byte
//
// 使用示例:
//
//	event := &CloudEvent[UserCreatedEvent]{
//	    CloudEventMetadata: CloudEventMetadata{
//	        Id:          "uuid-xxx",
//	        Source:      "urn:eventapi:client",
//	        SpecVersion: "1.0",
//	        Type:        "acme.user.created.v1",
//	        Subject:     "user.123",
//	        Extensions:  Extensions{}.WithString("trace_id", "abc"),
//	    },
//	    Data: UserCreatedEvent{...},
//	}
type CloudEvent[T any] struct {
	CloudEventMetadata
	Data T
}

// CloudEventMetadata 包含 CloudEvent 的元数据（标准属性）
// 参考: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
type CloudEventMetadata struct {
	// Id 是事件的唯一标识符，同一事件源产生的事件不能有重复 Id
	// 必填字段，格式: UUID v4
	// 参考: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#id
	Id string

	// Source 标识事件发生的上下文，通常是事件源的标识
	// 必填字段，格式: URI 或 URN
	// 例如: "urn:eventapi:service:user-service", "/myapp/users"
	// 参考: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#source
	Source string

	// SpecVersion 是 CloudEvents 规范的版本号
	// 必填字段，固定值: "1.0"
	// 参考: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#specversion
	SpecVersion string

	// Type 描述事件类型，用于路由、过滤和处理
	// 必填字段，格式: {reverse-domain}.{event-name}.{version}
	// 例如: "acme.user.created.v1", "io.github.order.paid.v2"
	// 参考: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#type
	Type string

	// DataContentType 描述事件数据的内容类型 (MIME type)
	// 可选字段，由 Codec 自动填充
	// 例如: "application/json", "application/protobuf"
	// 参考: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#datacontenttype
	DataContentType string

	// DataSchema 指向描述事件数据格式的 schema
	// 可选字段
	// 例如: "https://acme.com/schemas/user-created.json"
	// 参考: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#dataschema
	DataSchema string

	// Time 是事件生成的时间戳
	// 可选字段，由事件源在生成时填充
	// 参考: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#time
	Time time.Time

	// Subject 标识事件相关的资源或主题
	// 可选字段，用于消息路由和过滤
	// 从 proto 中标记为 (eventapi.v1.is_subject) = true 的字段提取
	// 例如: "user.123", "order.456"
	// 参考: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#subject
	Subject string

	// Extensions 是自定义扩展属性，用于传递协议无关的元数据
	// 可选字段，通过 Extensions.WithXxx() 方法添加
	// 常见扩展: trace_id, tenant_id, correlation_id
	// 参考: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#extension-context-attributes
	Extensions Extensions
}

// AttributeValue 是 CloudEvent 扩展属性值的包装类型
// 内部存储 any 类型值，支持类型安全的读取
//
// 使用示例:
//
//	val := NewAttributeValue("hello")
//	str, ok := val.AsString()  // "hello", true
//	num, ok := val.AsInt64()   // 0, false
type AttributeValue struct {
	value any
}

// NewAttributeValue 创建包装后的扩展属性值
func NewAttributeValue(v any) AttributeValue {
	return AttributeValue{value: v}
}

// AsBool 尝试将值转换为 bool，返回 (值, 是否成功)
func (v AttributeValue) AsBool() (bool, bool) {
	b, ok := v.value.(bool)
	return b, ok
}

// AsString 尝试将值转换为 string，返回 (值, 是否成功)
func (v AttributeValue) AsString() (string, bool) {
	s, ok := v.value.(string)
	return s, ok
}

// AsInt32 尝试将值转换为 int32，返回 (值, 是否成功)
func (v AttributeValue) AsInt32() (int32, bool) {
	i, ok := v.value.(int32)
	return i, ok
}

// AsInt64 尝试将值转换为 int64，返回 (值, 是否成功)
func (v AttributeValue) AsInt64() (int64, bool) {
	i, ok := v.value.(int64)
	return i, ok
}

// AsFloat64 尝试将值转换为 float64，返回 (值, 是否成功)
func (v AttributeValue) AsFloat64() (float64, bool) {
	f, ok := v.value.(float64)
	return f, ok
}

// AsBytes 尝试将值转换为 []byte，返回 (值, 是否成功)
func (v AttributeValue) AsBytes() ([]byte, bool) {
	b, ok := v.value.([]byte)
	return b, ok
}

// MarshalJSON 实现 JSON 序列化，自动解包内部值
func (v AttributeValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

// UnmarshalJSON 实现 JSON 反序列化，自动包装为 AttributeValue
func (v *AttributeValue) UnmarshalJSON(data []byte) error {
	var val any
	if err := json.Unmarshal(data, &val); err != nil {
		return err
	}
	v.value = val
	return nil
}

// Extensions 是 CloudEvent 扩展属性集合
// 支持链式调用设置多种类型的属性值
//
// 使用示例:
//
//	// 创建扩展属性
//	ext := Extensions{}.
//	    WithString("trace_id", "abc-123").
//	    WithInt64("timestamp", time.Now().Unix()).
//	    WithBool("is_test", true)
//
//	// 读取属性
//	traceId, ok := ext.Get("trace_id")
//	if ok {
//	    id, _ := traceId.AsString()
//	}
type Extensions map[string]AttributeValue

// WithBool 添加布尔类型的扩展属性
func (e Extensions) WithBool(key string, value bool) Extensions {
	e[key] = NewAttributeValue(value)
	return e
}

// WithString 添加字符串类型的扩展属性
func (e Extensions) WithString(key string, value string) Extensions {
	e[key] = NewAttributeValue(value)
	return e
}

// WithInt32 添加 int32 类型的扩展属性
func (e Extensions) WithInt32(key string, value int32) Extensions {
	e[key] = NewAttributeValue(value)
	return e
}

// WithInt64 添加 int64 类型的扩展属性
func (e Extensions) WithInt64(key string, value int64) Extensions {
	e[key] = NewAttributeValue(value)
	return e
}

// WithFloat64 添加 float64 类型的扩展属性
func (e Extensions) WithFloat64(key string, value float64) Extensions {
	e[key] = NewAttributeValue(value)
	return e
}

// WithBytes 添加 []byte 类型的扩展属性
func (e Extensions) WithBytes(key string, value []byte) Extensions {
	e[key] = NewAttributeValue(value)
	return e
}

// WithAny 添加任意类型的扩展属性
func (e Extensions) WithAny(key string, value any) Extensions {
	e[key] = NewAttributeValue(value)
	return e
}

// Get 获取扩展属性值，返回 (值, 是否存在)
func (e Extensions) Get(key string) (AttributeValue, bool) {
	v, ok := e[key]
	return v, ok
}

// Keys 返回所有扩展属性的 key 列表
func (e Extensions) Keys() []string {
	keys := make([]string, 0, len(e))
	for k := range e {
		keys = append(keys, k)
	}
	return keys
}
