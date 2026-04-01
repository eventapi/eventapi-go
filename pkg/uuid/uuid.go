// Package uuid 提供 UUID 生成功能
//
// 直接使用 Google UUID 库实现
// 参考: https://github.com/google/uuid
package uuid

import (
	"github.com/google/uuid"
)

// Generate 生成新的 UUID v4 字符串
// 使用 Google UUID 库确保随机性和唯一性
func Generate() string {
	return uuid.New().String()
}
