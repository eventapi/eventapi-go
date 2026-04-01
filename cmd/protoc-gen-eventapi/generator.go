// generator.go 负责将解析后的 ServiceInfo 渲染为 Go 代码
//
// 设计思想:
// 1. 模板驱动 - 使用 text/template 分离代码生成逻辑和代码模板
// 2. 函数映射 - 通过 template.FuncMap 提供辅助函数（类型转换、命名转换）
// 3. 协议感知 - 根据 protocol 字段自动选择对应的 transport 包
// 4. 类型安全 - 生成的代码使用泛型确保 transport 类型匹配
//
// 模板系统:
// - client.tmpl: 主模板，生成客户端接口、实现、Builder
// - nested_message.tmpl: 嵌套消息结构体
//
// 辅助函数:
// - lower: 字符串转小写
// - snakecase: CamelCase 转 snake_case
// - transportConfig: protocol -> transport.Config 类型名
// - transportChannel: protocol -> transport.Channel 类型名
// - transportPackage: protocol -> import 包名
//
// 代码结构:
// 每个 EventAPI service 生成一个目录，包含:
// - client.go: 客户端代码
// - <message>.go: 嵌套消息结构体（如果有）
package main

import (
	"embed"
	"fmt"
	"strings"
	"text/template"

	"google.golang.org/protobuf/compiler/protogen"
)

//go:embed templates/*.tmpl
var templatesFS embed.FS

var templates *template.Template

func init() {
	funcMap := template.FuncMap{
		"lower":               strings.ToLower,
		"snakecase":           toSnakeCase,
		"capitalize":          capitalize,
		"protocolName":        toProtocolName,
		"transportConfig":     toTransportConfig,
		"transportChannel":    toTransportChannel,
		"transportPackage":    toTransportPackage,
		"builderType":         toBuilderType,
		"builderConstructor":  toBuilderConstructor,
		"getSubjectField":     getSubjectField,
		"buildChannelBinding": buildChannelBinding,
		"channelAddrComment":  channelAddrComment,
	}
	templates = template.Must(template.New("").Funcs(funcMap).ParseFS(templatesFS, "templates/*.tmpl"))
}

// channelAddrComment 根据协议返回通道地址的注释
func channelAddrComment(protocol string) string {
	switch strings.ToLower(protocol) {
	case "amqp":
		return "Queue"
	case "rocketmq":
		return "Topic"
	default:
		return "Topic"
	}
}

// toTransportConfig 将 protocol 映射到对应的 transport.Config 类型名
func toTransportConfig(protocol string) string {
	switch strings.ToLower(protocol) {
	case "kafka":
		return "kafka.Config"
	case "rocketmq":
		return "rocketmq.Config"
	case "amqp":
		return "amqp.Config"
	default:
		return "kafka.Config"
	}
}

// toTransportChannel 将 protocol 映射到对应的 transport.Channel 类型名
func toTransportChannel(protocol string) string {
	switch strings.ToLower(protocol) {
	case "kafka":
		return "kafka.Channel"
	case "rocketmq":
		return "rocketmq.Channel"
	case "amqp":
		return "amqp.Channel"
	default:
		return "kafka.Channel"
	}
}

// toTransportPackage 将 protocol 映射到对应的 import 包名
func toTransportPackage(protocol string) string {
	switch strings.ToLower(protocol) {
	case "kafka":
		return "kafka"
	case "rocketmq":
		return "rocketmq"
	case "amqp":
		return "amqp"
	default:
		return "kafka"
	}
}

func toBuilderType(protocol string) string {
	switch strings.ToLower(protocol) {
	case "kafka":
		return "kafka.EventBuilder"
	case "rocketmq":
		return "rocketmq.EventBuilder"
	case "amqp":
		return "amqp.EventBuilder"
	default:
		return "kafka.EventBuilder"
	}
}

func toBuilderConstructor(protocol string) string {
	switch strings.ToLower(protocol) {
	case "kafka":
		return "kafka.NewEventBuilder"
	case "rocketmq":
		return "rocketmq.NewEventBuilder"
	case "amqp":
		return "amqp.NewEventBuilder"
	default:
		return "kafka.NewEventBuilder"
	}
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func toProtocolName(protocol string) string {
	switch strings.ToLower(protocol) {
	case "rocketmq":
		return "RocketMQ"
	case "kafka":
		return "Kafka"
	case "amqp":
		return "AMQP"
	default:
		return "Kafka"
	}
}

// toSnakeCase 将 CamelCase 转换为 snake_case
func toSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteRune('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

// toCamelCase 将 snake_case 转换为 CamelCase
func toCamelCase(s string) string {
	var result strings.Builder
	upperNext := false
	for _, r := range s {
		if r == '_' {
			upperNext = true
		} else if upperNext {
			result.WriteRune(r - 'a' + 'A')
			upperNext = false
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// getSubjectField 获取消息中标记为 subject 的字段名
func getSubjectField(msg MessageInfo) string {
	for _, f := range msg.Fields {
		if f.IsSubject {
			return f.Name
		}
	}
	return ""
}

// buildChannelBinding 构建 Channel Binding 配置字符串
func buildChannelBinding(protocol string, channel ChannelInfo) string {
	switch strings.ToLower(protocol) {
	case "kafka":
		if channel.Binding.Kafka != nil && channel.Binding.Kafka.Key != "" {
			return fmt.Sprintf("Binding: &eventapiv1.OperationRule_Channel_Binding{Kafka: &eventapiv1.OperationRule_Channel_Binding_Kafka{Key: \"%s\"}},", channel.Binding.Kafka.Key)
		}
	case "rocketmq":
		if channel.Binding.Rocketmq != nil {
			parts := []string{"Binding: &eventapiv1.OperationRule_Channel_Binding{Rocketmq: &eventapiv1.OperationRule_Channel_Binding_Rocketmq{"}
			rmq := channel.Binding.Rocketmq
			if rmq.Tag != "" {
				parts = append(parts, fmt.Sprintf("Tag: \"%s\", ", rmq.Tag))
			}
			if rmq.Key != "" {
				parts = append(parts, fmt.Sprintf("Key: \"%s\", ", rmq.Key))
			}
			if rmq.MessageType != "" {
				parts = append(parts, fmt.Sprintf("MessageType: eventapiv1.OperationRule_Channel_Binding_Rocketmq_%s, ", rmq.MessageType))
			}
			if rmq.DelayLevel > 0 {
				parts = append(parts, fmt.Sprintf("DelayLevel: %d, ", rmq.DelayLevel))
			}
			if len(parts) > 1 {
				return strings.Join(parts, "")[:len(strings.Join(parts, ""))-2] + "}},"
			}
			return "Binding: &eventapiv1.OperationRule_Channel_Binding{Rocketmq: &eventapiv1.OperationRule_Channel_Binding_Rocketmq{}},"
		}
	case "amqp":
		if channel.Binding.Amqp != nil && (channel.Binding.Amqp.Exchange != "" || channel.Binding.Amqp.RoutingKey != "") {
			parts := []string{"Binding: &eventapiv1.OperationRule_Channel_Binding{Amqp: &eventapiv1.OperationRule_Channel_Binding_Amqp{"}
			amqp := channel.Binding.Amqp
			if amqp.Exchange != "" {
				parts = append(parts, fmt.Sprintf("Exchange: \"%s\", ", amqp.Exchange))
			}
			if amqp.RoutingKey != "" {
				parts = append(parts, fmt.Sprintf("RoutingKey: \"%s\", ", amqp.RoutingKey))
			}
			if len(parts) > 1 {
				return strings.Join(parts, "")[:len(strings.Join(parts, ""))-2] + "}},"
			}
		}
	}
	return ""
}

// GeneratorData 传递给模板的数据结构
type GeneratorData struct {
	ServiceName         string
	PackageName         string
	Protocol            string
	RPCs                []RPCInfo
	UniqueMessages      []MessageInfo
	SendMessages        []MessageInfo
	SendMessageNames    map[string]bool
	ReceiveMessages     []MessageInfo
	ReceiveMessageNames map[string]bool
	NestedMessages      map[string]MessageInfo
	GoImportPath        string
}

// generateFile 为每个 proto 文件生成代码
func generateFile(gen *protogen.Plugin, file *protogen.File) error {
	if len(file.Services) == 0 {
		return nil
	}

	services, err := ParseFile(file)
	if err != nil {
		return err
	}

	if len(services) == 0 {
		return nil
	}

	for _, svc := range services {
		if err := generateService(gen, file, svc); err != nil {
			return err
		}
	}

	return nil
}

// generateService 为单个 EventAPI service 生成代码
func generateService(gen *protogen.Plugin, file *protogen.File, svc ServiceInfo) error {
	// 为每个协议生成一个 SDK
	for _, p := range svc.Protocols {
		if err := generateSDKForProtocol(gen, file, svc, p.Name, svc.RPCs, string(file.GoPackageName)); err != nil {
			return err
		}
	}

	return nil
}

// generateSDKForProtocol 为指定协议生成 SDK
func generateSDKForProtocol(
	gen *protogen.Plugin,
	file *protogen.File,
	svc ServiceInfo,
	protocol string,
	rpcs []RPCInfo,
	pkgName string,
) error {
	seenMessages := make(map[string]bool)
	var uniqueMessages []MessageInfo
	sendMessageNames := make(map[string]bool)
	var sendMessages []MessageInfo
	receiveMessageNames := make(map[string]bool)
	var receiveMessages []MessageInfo

	seenSendRPCs := make(map[string]bool)
	var dedupedRPCs []RPCInfo

	for _, rpc := range rpcs {
		if rpc.Operation == "SEND" {
			if seenSendRPCs[rpc.InputType] {
				continue
			}
			seenSendRPCs[rpc.InputType] = true
		}
		dedupedRPCs = append(dedupedRPCs, rpc)

		if rpc.MessageDef != nil && !seenMessages[rpc.InputType] {
			seenMessages[rpc.InputType] = true
			uniqueMessages = append(uniqueMessages, *rpc.MessageDef)
		}
		if rpc.Operation == "SEND" && rpc.MessageDef != nil {
			if !sendMessageNames[rpc.InputType] {
				sendMessageNames[rpc.InputType] = true
				sendMessages = append(sendMessages, *rpc.MessageDef)
			}
		}
		if rpc.Operation == "RECEIVE" && rpc.MessageDef != nil {
			if !receiveMessageNames[rpc.InputType] {
				receiveMessageNames[rpc.InputType] = true
				receiveMessages = append(receiveMessages, *rpc.MessageDef)
			}
		}
	}

	data := GeneratorData{
		ServiceName:         svc.Name,
		PackageName:         pkgName,
		Protocol:            protocol,
		RPCs:                dedupedRPCs,
		UniqueMessages:      uniqueMessages,
		SendMessages:        sendMessages,
		SendMessageNames:    sendMessageNames,
		ReceiveMessages:     receiveMessages,
		ReceiveMessageNames: receiveMessageNames,
		NestedMessages:      svc.NestedMessage,
		GoImportPath:        string(file.GoImportPath),
	}

	// 生成主客户端文件
	if err := generateClientFile(gen, file, data); err != nil {
		return err
	}

	return nil
}

// generateNestedMessageFile 为嵌套消息生成单独的文件
func generateNestedMessageFile(gen *protogen.Plugin, file *protogen.File, data GeneratorData, name string, msg MessageInfo) error {
	serviceDir := toSnakeCase(data.ServiceName)
	filename := serviceDir + "/" + toSnakeCase(name) + ".go"
	g := gen.NewGeneratedFile(filename, file.GoImportPath)

	g.P("// Code generated by protoc-gen-eventapi. DO NOT EDIT.")
	g.P()
	g.P("package ", file.GoPackageName)
	g.P()

	if err := templates.ExecuteTemplate(g, "nested_message", msg); err != nil {
		return err
	}

	return nil
}

// generateClientFile 生成客户端主文件
func generateClientFile(gen *protogen.Plugin, file *protogen.File, data GeneratorData) error {
	// 两层包结构: {service_name}/{protocol}/
	// 例如: multi_protocol_event_bus/kafka/, multi_protocol_event_bus/amqp/
	serviceDir := toSnakeCase(data.ServiceName)
	protocolDir := strings.ToLower(data.Protocol)
	filename := serviceDir + "/" + protocolDir + "/" + toSnakeCase(data.ServiceName) + ".go"
	g := gen.NewGeneratedFile(filename, file.GoImportPath)

	// 包名 = service名 + protocol名 (全小写)
	pkgName := strings.ToLower(toSnakeCase(data.ServiceName) + "_" + data.Protocol)

	g.P("// Code generated by protoc-gen-eventapi. DO NOT EDIT.")
	g.P()
	g.P("package ", pkgName)
	g.P()
	g.P("import (")
	g.P("    \"context\"")
	g.P("    \"fmt\"")
	g.P("    \"time\"")
	g.P()
	g.P("    \"github.com/eventapi/eventapi-go/pkg/cloudevent\"")
	g.P("    \"github.com/eventapi/eventapi-go/pkg/codec\"")
	g.P("    \"github.com/eventapi/eventapi-go/pkg/options\"")
	g.P("    \"github.com/eventapi/eventapi-go/pkg/transport\"")
	g.P("    \"github.com/eventapi/eventapi-go/pkg/transport/", toTransportPackage(data.Protocol), "\"")
	g.P("    \"github.com/eventapi/eventapi-go/pkg/uuid\"")

	// 只有存在 binding 时才导入 eventapiv1
	if hasBinding(data.RPCs, data.Protocol) {
		g.P("    eventapiv1 \"github.com/eventapi/eventapi-go/pkg/eventapi/v1\"")
	}

	g.P(")")
	g.P()

	if err := templates.ExecuteTemplate(g, "client", data); err != nil {
		return err
	}

	return nil
}

// hasBinding 检查是否有任何 RPC 包含 binding
func hasBinding(rpcs []RPCInfo, protocol string) bool {
	for _, rpc := range rpcs {
		channel := rpc.Channel
		switch strings.ToLower(protocol) {
		case "kafka":
			if channel.Binding.Kafka != nil && channel.Binding.Kafka.Key != "" {
				return true
			}
		case "rocketmq":
			if channel.Binding.Rocketmq != nil {
				return true
			}
		case "amqp":
			if channel.Binding.Amqp != nil && (channel.Binding.Amqp.Exchange != "" || channel.Binding.Amqp.RoutingKey != "") {
				return true
			}
		}
	}
	return false
}
