// parser.go 负责解析 proto AST，提取 EventAPI 相关的元数据
//
// 解析流程:
// 1. ParseFile - 入口函数，遍历所有 service 和 message
// 2. parseService - 提取 service 级别的 server option（protocol）
// 3. parseMethod - 提取 method 级别的 asyncapi option（channel、operation）
// 4. parseMessage - 将 proto message 转换为 Go 结构体定义
// 5. 构建 ServiceInfo 树，供 generator 使用
//
// 数据结构:
// - ServiceInfo: 一个 asyncapi 服务（对应 proto service）
// - RPCInfo: 一个事件操作（对应 proto method）
// - ChannelInfo: 消息通道配置（topic/queue 地址、绑定参数）
// - MessageInfo: 事件消息结构（字段、类型、tag）
//
// Option 扩展:
// - eventapi.v1.server: 定义服务协议（kafka/amqp/mqtt）
// - eventapi.v1.asyncapi: 定义通道和操作类型（send/receive）
// - eventapi.v1.event_type: 定义 CloudEvents type 属性
package main

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	eventapiv1 "github.com/eventapi/eventapi-go/pkg/eventapi/v1"
)

// ServiceInfo 存储一个 AsyncAPI 服务的完整信息
type ServiceInfo struct {
	Name          string
	Protocols     []ProtocolInfo         // 支持的协议列表
	RPCs          []RPCInfo              // 该服务的所有事件操作
	AllMessages   []MessageInfo          // 所有消息定义
	NestedMessage map[string]MessageInfo // 嵌套消息映射
}

// ProtocolInfo 协议信息
type ProtocolInfo struct {
	Name    string // 协议名称: kafka, amqp, mqtt...
	Version string // 协议版本
}

// RPCInfo 存储单个事件操作的信息
type RPCInfo struct {
	Name       string       // 方法名（如 SendUserCreated）
	Operation  string       // 操作类型：SEND 或 RECEIVE
	Channel    ChannelInfo  // 通道配置
	EventType  string       // CloudEvents type（如 user.created.v1）
	InputType  string       // 输入消息类型名
	MessageDef *MessageInfo // 消息定义详情
	Protocol   string       // 协议（可覆盖 service 级别）
}

// ChannelInfo 存储消息通道的配置
type ChannelInfo struct {
	Address     string             // 通道地址（如 topic 名称）
	Description string             // 描述
	Binding     ChannelBindingInfo // 协议特定的绑定配置
}

// ChannelBindingInfo 存储协议特定的绑定参数
type ChannelBindingInfo struct {
	Kafka    *KafkaBinding
	Amqp     *AmqpBinding
	Mqtt     *MqttBinding
	Nats     *NatsBinding
	Rocketmq *RocketmqBinding
}

// KafkaBinding Kafka 特定的绑定配置
type KafkaBinding struct {
	Key       string // 分区 key
	Partition int32  // 指定分区
}

// AmqpBinding AMQP 特定的绑定配置
type AmqpBinding struct {
	Exchange   string // 交换机名称
	RoutingKey string // 路由 key
}

// MqttBinding MQTT 特定的绑定配置
type MqttBinding struct {
	QoS    int32 // 服务质量等级
	Retain bool  // 是否保留消息
}

// NatsBinding NATS 特定的绑定配置
type NatsBinding struct{}

// RocketmqBinding RocketMQ 特定的绑定配置
type RocketmqBinding struct {
	Tag         string // 消息 Tag，用于过滤
	Key         string // 消息 Key，用于查询
	MessageType string // 消息类型: NORMAL, ORDER, TRANSACTION, DELAY
	DelayLevel  int32  // 延时级别
}

// MessageInfo 存储消息结构体的信息
type MessageInfo struct {
	Name    string      // 结构体名
	Comment string      // 注释
	Fields  []FieldInfo // 字段列表
}

// FieldInfo 存储单个字段的信息
type FieldInfo struct {
	Name      string // 字段名（Go 风格）
	Type      string // Go 类型
	JSONTag   string // JSON tag（proto 原始字段名）
	Optional  bool   // 是否可选
	Comment   string // 注释
	IsSubject bool   // 是否是 CloudEvent subject
}

// ParseFile 解析 proto 文件，提取所有 EventAPI 服务信息
func ParseFile(file *protogen.File) ([]ServiceInfo, error) {
	// 收集所有嵌套消息定义
	nestedMessages := make(map[string]MessageInfo)
	for _, msg := range file.Messages {
		msgInfo := parseMessage(msg)
		nestedMessages[msgInfo.Name] = *msgInfo
	}

	var services []ServiceInfo

	// 解析每个 service
	for _, service := range file.Services {
		svc, err := parseService(service, nestedMessages)
		if err != nil {
			return nil, fmt.Errorf("parse service %s: %w", service.Desc.Name(), err)
		}
		if svc != nil {
			services = append(services, *svc)
		}
	}

	return services, nil
}

// parseService 解析单个 service，提取协议配置和 RPC 定义
func parseService(service *protogen.Service, nestedMessages map[string]MessageInfo) (*ServiceInfo, error) {
	var protocols []ProtocolInfo

	// 提取 service 级别的 server option
	if service.Desc.Options() != nil {
		if proto.HasExtension(service.Desc.Options(), eventapiv1.E_Server) {
			serverRule := proto.GetExtension(service.Desc.Options(), eventapiv1.E_Server).(*eventapiv1.AsyncApiServerRule)
			for _, p := range serverRule.GetProtocols() {
				protocols = append(protocols, ProtocolInfo{
					Name:    p.GetName(),
					Version: p.GetVersion(),
				})
			}
		}
	}

	// 没有 protocol option 的 service 不是 EventAPI 服务
	if len(protocols) == 0 {
		return nil, nil
	}

	// 默认使用第一个协议
	defaultProtocol := protocols[0].Name

	svc := &ServiceInfo{
		Name:          string(service.Desc.Name()),
		Protocols:     protocols,
		NestedMessage: nestedMessages,
	}

	// 解析每个 method
	for _, method := range service.Methods {
		rpc, err := parseMethod(method, defaultProtocol)
		if err != nil {
			return nil, fmt.Errorf("parse method %s: %w", method.Desc.Name(), err)
		}
		if rpc != nil {
			svc.RPCs = append(svc.RPCs, *rpc)
			if rpc.MessageDef != nil {
				svc.AllMessages = append(svc.AllMessages, *rpc.MessageDef)
			}
		}
	}

	return svc, nil
}

// parseMethod 解析单个 method，提取 operation option 配置
func parseMethod(method *protogen.Method, protocol string) (*RPCInfo, error) {
	var rule *eventapiv1.OperationRule

	// 提取 method 级别的 operation option
	if method.Desc.Options() != nil {
		if proto.HasExtension(method.Desc.Options(), eventapiv1.E_Operation) {
			rule = proto.GetExtension(method.Desc.Options(), eventapiv1.E_Operation).(*eventapiv1.OperationRule)
		}
	}

	// 没有 operation option 的 method 不是事件操作
	if rule == nil {
		return nil, nil
	}

	channel := parseChannel(rule.GetChannel())

	rpc := &RPCInfo{
		Name:      string(method.Desc.Name()),
		Operation: rule.GetAction().String(),
		Channel:   channel,
	}

	// 解析输入消息类型和事件类型
	if method.Input != nil {
		rpc.InputType = string(method.Input.Desc.Name())
		rpc.MessageDef = parseMessage(method.Input)

		// 提取 event_type option
		if method.Input.Desc.Options() != nil {
			if proto.HasExtension(method.Input.Desc.Options(), eventapiv1.E_EventType) {
				eventType := proto.GetExtension(method.Input.Desc.Options(), eventapiv1.E_EventType).(string)
				rpc.EventType = eventType
			}
		}
	}

	return rpc, nil
}

// parseChannel 解析通道配置
func parseChannel(channel *eventapiv1.OperationRule_Channel) ChannelInfo {
	if channel == nil {
		return ChannelInfo{}
	}

	info := ChannelInfo{
		Address:     channel.GetAddress(),
		Description: channel.GetDescription(),
	}

	if channel.GetBinding() != nil {
		info.Binding = parseChannelBinding(channel.GetBinding())
	}

	return info
}

// parseChannelBinding 解析通道绑定配置
func parseChannelBinding(binding *eventapiv1.OperationRule_Channel_Binding) ChannelBindingInfo {
	info := ChannelBindingInfo{}

	if binding.GetKafka() != nil {
		kafka := binding.GetKafka()
		info.Kafka = &KafkaBinding{
			Key:       kafka.GetKey(),
			Partition: kafka.GetPartition(),
		}
	}

	if binding.GetAmqp() != nil {
		amqpBinding := binding.GetAmqp()
		info.Amqp = &AmqpBinding{
			Exchange:   amqpBinding.GetExchange(),
			RoutingKey: amqpBinding.GetRoutingKey(),
		}
	}

	if binding.GetMqtt() != nil {
		mqtt := binding.GetMqtt()
		info.Mqtt = &MqttBinding{
			QoS:    mqtt.GetQos(),
			Retain: mqtt.GetRetain(),
		}
	}

	if binding.GetRocketmq() != nil {
		rmq := binding.GetRocketmq()
		info.Rocketmq = &RocketmqBinding{
			Tag:         rmq.GetTag(),
			Key:         rmq.GetKey(),
			MessageType: rmq.GetMessageType().String(),
			DelayLevel:  rmq.GetDelayLevel(),
		}
	}

	return info
}

// parseMessage 解析 proto message 为 Go 结构体信息
func parseMessage(msg *protogen.Message) *MessageInfo {
	info := &MessageInfo{
		Name:    string(msg.Desc.Name()),
		Comment: getMessageComment(msg),
		Fields:  make([]FieldInfo, 0, len(msg.Fields)),
	}

	for _, field := range msg.Fields {
		fieldName := string(field.Desc.Name())
		f := FieldInfo{
			Name:      toGoFieldName(fieldName),
			Type:      goType(field),
			JSONTag:   fieldName,
			Optional:  field.Desc.HasOptionalKeyword(),
			Comment:   getFieldComment(field),
			IsSubject: isSubjectField(field),
		}
		info.Fields = append(info.Fields, f)
	}

	return info
}

// isSubjectField 检查字段是否有 (eventapi.v1.is_subject) = true
func isSubjectField(field *protogen.Field) bool {
	if field.Desc.Options() != nil {
		return proto.GetExtension(field.Desc.Options(), eventapiv1.E_IsSubject).(bool)
	}
	return false
}

// getMessageComment 提取消息注释
func getMessageComment(msg *protogen.Message) string {
	comment := msg.Comments.Leading.String()
	comment = strings.TrimSpace(comment)
	comment = strings.TrimPrefix(comment, "//")
	return strings.TrimSpace(comment)
}

// getFieldComment 提取字段注释
func getFieldComment(field *protogen.Field) string {
	comment := field.Comments.Leading.String()
	comment = strings.TrimSpace(comment)
	comment = strings.TrimPrefix(comment, "//")
	return strings.TrimSpace(comment)
}

// toGoFieldName 将 snake_case 转换为 CamelCase
func toGoFieldName(s string) string {
	parts := strings.Split(s, "_")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}

// goType 将 proto 类型转换为 Go 类型
func goType(field *protogen.Field) string {
	desc := field.Desc
	kind := desc.Kind()

	if field.Desc.IsList() {
		elemType := goScalarType(kind)
		if kind == protoreflect.MessageKind {
			return "[]" + getMessageTypeName(desc.Message())
		}
		return "[]" + elemType
	}

	if field.Desc.IsMap() {
		keyType := goScalarType(desc.MapKey().Kind())
		valKind := desc.MapValue().Kind()
		valType := goScalarType(valKind)
		if valKind == protoreflect.MessageKind {
			valType = getMessageTypeName(desc.MapValue().Message())
		}
		return fmt.Sprintf("map[%s]%s", keyType, valType)
	}

	if kind == protoreflect.MessageKind {
		return getMessageTypeName(desc.Message())
	}

	return goScalarType(kind)
}

// getMessageTypeName 获取消息类型的 Go 名称
func getMessageTypeName(msg protoreflect.MessageDescriptor) string {
	fullName := string(msg.FullName())

	if fullName == "google.protobuf.Timestamp" {
		return "time.Time"
	}
	if fullName == "google.protobuf.Duration" {
		return "time.Duration"
	}
	if fullName == "google.protobuf.Any" {
		return "map[string]any"
	}
	if fullName == "google.protobuf.Struct" {
		return "map[string]any"
	}
	if fullName == "google.protobuf.Value" {
		return "any"
	}

	return string(msg.Name())
}

// goScalarType 标量类型映射
func goScalarType(kind protoreflect.Kind) string {
	switch kind {
	case protoreflect.StringKind:
		return "string"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return "int32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return "int64"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "uint32"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "uint64"
	case protoreflect.FloatKind:
		return "float32"
	case protoreflect.DoubleKind:
		return "float64"
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.BytesKind:
		return "[]byte"
	case protoreflect.EnumKind:
		return "int32"
	default:
		return "any"
	}
}
