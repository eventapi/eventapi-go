// protoc-gen-eventapi 是一个 Protocol Buffers 插件，用于生成 EventAPI 客户端代码
//
// 设计思想:
// 1. 声明式事件定义 - 通过 proto 文件和 option 扩展定义事件拓扑
// 2. 代码生成 - 将 proto 定义转换为类型安全的 Go 客户端 SDK
// 3. CloudEvents 兼容 - 自动生成符合 CloudEvents 规范的事件结构
// 4. 多协议支持 - 通过 protocol 字段支持 Kafka、AMQP、MQTT 等多种传输协议
//
// 工作原理:
// 1. protoc 解析 proto 文件，将 AST 通过 stdin 传递给本插件
// 2. 本插件使用 protogen 包读取 AST 和自定义 option
// 3. 根据 service 的 protocol 和 method 的 asyncapi option 生成对应代码
// 4. 输出 Go 代码到 stdout，由 protoc 写入文件
//
// 使用方式:
//
//	protoc --plugin=protoc-gen-eventapi --eventapi_out=. --eventapi_opt=paths=source_relative -I. *.proto
//
// 生成内容:
// - 事件结构体（从 proto message 生成）
// - 客户端接口和实现
// - Builder 模式用于链式构建事件
// - CloudEvent 转换和发送逻辑
package main

import (
	"flag"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	var flags flag.FlagSet

	opts := protogen.Options{
		ParamFunc: flags.Set,
	}

	opts.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)

		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}

			if err := generateFile(gen, f); err != nil {
				return err
			}
		}

		return nil
	})
}
