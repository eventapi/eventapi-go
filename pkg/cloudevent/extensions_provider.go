package cloudevent

import "context"

// ExtensionsProvider 定义了在发送事件时如何注入自定义 CloudEvent 扩展属性。
// 返回值中的 key 不需要带 "ce_" 前缀。
type ExtensionsProvider = func(ctx context.Context) Extensions
