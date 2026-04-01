// Package codec 提供事件序列化和反序列化接口
//
// 支持的编解码格式:
// - JSON: 应用广泛，可读性好，适合调试
// - Protobuf: 高效紧凑，适合高性能场景
//
// 使用示例:
//
//	// JSON 编解码
//	jsonCodec := codec.NewJSON()
//	data, _ := jsonCodec.Encode(event)
//	var evt MyEvent
//	jsonCodec.Decode(data, &evt)
//
//	// Protobuf 编解码
//	protoCodec := codec.NewProto()
//	data, _ := protoCodec.Encode(protoEvent)
package codec

// Codec 定义事件编解码器接口
// 实现者负责将 Go 结构体序列化为字节流，以及反向反序列化
type Codec interface {
	// Encode 将值序列化为字节
	// v 可以是任意类型，具体支持类型取决于实现
	Encode(v any) ([]byte, error)

	// Decode 将字节反序列化为指定值
	// data 是序列化后的字节
	// v 必须是指针，用于接收反序列化结果
	Decode(data []byte, v any) error

	// ContentType 返回此编解码器的 MIME 类型
	// 例如: "application/json", "application/protobuf"
	ContentType() string
}
