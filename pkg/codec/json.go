package codec

import "encoding/json"

// JSON 实现 Codec 接口，使用 JSON 格式编解码
type JSON struct{}

// NewJSON 创建新的 JSON 编解码器
func NewJSON() Codec {
	return &JSON{}
}

// Encode 使用 json.Marshal 序列化值为 JSON
func (c *JSON) Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Decode 使用 json.Unmarshal 将 JSON 反序列化到 v
func (c *JSON) Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// ContentType 返回 JSON 的 MIME 类型
func (c *JSON) ContentType() string {
	return "application/json"
}
