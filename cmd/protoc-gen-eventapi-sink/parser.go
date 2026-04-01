package main

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"

	eventapiv1 "github.com/eventapi/eventapi-go/pkg/eventapi/v1"
)

// =====================================================================
// 类型推断方案 (Strict Mode)
// =====================================================================
//
// 优先级:
//   1. 显式指定 (用户写了 type) -> 直接用
//   2. Data 字段 (查 Proto 定义) -> 准确映射 (int64 -> BIGINT)
//   3. 标准 Header (查内置映射表) -> 准确映射 (ce_time -> TIMESTAMP)
//   4. 无法推断 -> 返回 Error, 终止生成
//
// 设计原则: Fail-Fast, 避免隐式 Fallback 到 VARCHAR 导致隐性错误
// =====================================================================

// inferType 推断字段类型 (Strict Mode: 无法推断时返回 Error)
//
// 优先级:
//  1. 显式指定 (用户写了 type) -> 直接用
//  2. Data 字段 (查 Proto 定义) -> 准确映射
//  3. 标准 Header (查内置映射表) -> 准确映射
//  4. 无法推断 -> 返回 Error
func inferType(m MappingInfo, protoFields map[string]ProtoFieldInfo) (string, error) {
	// 1. 显式指定
	if m.Type != "" && m.Type != "COLUMN_TYPE_UNSPECIFIED" {
		return m.Type, nil
	}

	// 2. Data 字段: 查 Proto 定义
	if m.IsData && m.DataPath != "" && protoFields != nil {
		fieldName := strings.TrimPrefix(m.DataPath, "$.")
		if pf, ok := protoFields[fieldName]; ok {
			return protoTypeToColumnType(pf.ProtoType, pf.IsMap, pf.IsMessage, pf.IsEnum), nil
		}
	}

	// 3. Header 字段: 查内置映射表
	if m.IsHeader {
		if t, ok := cloudEventHeaderTypes[strings.ToLower(m.HeaderPath)]; ok {
			return t, nil
		}
	}

	// 4. 无法推断 -> Error
	return "", fmt.Errorf("unable to infer type for field '%s'. Please specify 'type' explicitly", m.HeaderPath+m.DataPath)
}

// cloudEventHeaderTypes CloudEvent 标准头类型映射表
var cloudEventHeaderTypes = map[string]string{
	"ce_id":          "COLUMN_TYPE_VARCHAR",
	"ce_source":      "COLUMN_TYPE_VARCHAR",
	"ce_specversion": "COLUMN_TYPE_VARCHAR",
	"ce_type":        "COLUMN_TYPE_VARCHAR",
	"ce_time":        "COLUMN_TYPE_TIMESTAMP",
	"ce_subject":     "COLUMN_TYPE_VARCHAR",
	"ce_dataschema":  "COLUMN_TYPE_VARCHAR",
}

// SinkInfo 从 SinkRule 选项提取的信息
type SinkInfo struct {
	Target               string
	Database             string
	Table                string
	PrimaryKey           []string
	PartitionKey         []string
	Mappings             []MappingInfo
	InferredFields       []MappingInfo // 自动推断的字段
	Checkpoint           string
	BatchSize            int32
	Parallelism          int32
	Properties           map[string]string
	SourceTopic          string
	EventDataContentType string
	AutoInferDataMapping bool // 是否自动推断 data_path 映射
	AutoMap              bool // 是否自动映射所有字段
}

// MappingInfo 单个字段映射信息
type MappingInfo struct {
	HeaderPath    string // header_path 原始值
	HeaderName    string // 实际 header 键名，用于 METADATA 列
	DataPath      string // data_path 值
	TargetColumn  string
	Type          string
	NotNull       bool
	Comment       string
	IsHeader      bool
	IsData        bool
	IsNestedJSON  bool   // header 值为 JSON，需嵌套提取
	NestedJSONKey string // 嵌套 JSON 路径，如 "$.tenant"
	Ignore        bool   // 是否忽略该字段（不落盘）
}

// ProtoFieldInfo 从 Proto Message 字段提取的信息
type ProtoFieldInfo struct {
	Name       string // snake_case 字段名
	GoName     string // Go 字段名
	ProtoType  string // proto 字段类型（如 string, int64, google.protobuf.Timestamp）
	Comment    string // 字段注释
	IsOptional bool   // 是否 optional
	IsMap      bool   // 是否 map 类型
	IsMessage  bool   // 是否 message 类型
	IsEnum     bool   // 是否 enum 类型
	IsHeader   bool   // 是否标记为 is_event_header
}

// ParseSinks 从 proto 文件提取所有 SinkRule 定义
func ParseSinks(file *protogen.File) ([]SinkInfo, error) {
	var sinks []SinkInfo
	for _, svc := range file.Services {
		for _, method := range svc.Methods {
			if method.Desc.Options() == nil {
				continue
			}
			if !proto.HasExtension(method.Desc.Options(), eventapiv1.E_Sink) {
				continue
			}
			rule := proto.GetExtension(method.Desc.Options(), eventapiv1.E_Sink).(*eventapiv1.SinkRule)
			sink, err := parseSinkRule(method, rule, file)
			if err != nil {
				return nil, err
			}
			sinks = append(sinks, sink)
		}
	}
	return sinks, nil
}

func parseSinkRule(method *protogen.Method, rule *eventapiv1.SinkRule, file *protogen.File) (SinkInfo, error) {
	sink := SinkInfo{
		Target:               rule.GetTarget().String(),
		Database:             rule.GetDatabase(),
		Table:                rule.GetTable(),
		PrimaryKey:           rule.GetPrimaryKey(),
		PartitionKey:         rule.GetPartitionKey(),
		SourceTopic:          rule.GetSourceTopic(),
		EventDataContentType: rule.GetEventDataContentType().String(),
		AutoInferDataMapping: rule.AutoInferDataMapping == nil || rule.GetAutoInferDataMapping(),
		AutoMap:              rule.GetAutoMap(),
	}

	// 如果 sink 未指定 event_data_content_type，从 Message 继承
	if sink.EventDataContentType == "" || sink.EventDataContentType == "CONTENT_TYPE_UNSPECIFIED" {
		if method != nil && method.Input != nil {
			if method.Input.Desc.Options() != nil {
				if proto.HasExtension(method.Input.Desc.Options(), eventapiv1.E_EventDataContentType) {
					dct := proto.GetExtension(method.Input.Desc.Options(), eventapiv1.E_EventDataContentType).(eventapiv1.EventDataContentType)
					sink.EventDataContentType = dct.String()
				}
			}
		}
	}

	if rule.GetConfig() != nil {
		cfg := rule.GetConfig()
		sink.Checkpoint = cfg.GetCheckpointInterval()
		sink.BatchSize = cfg.GetBatchSize()
		sink.Parallelism = cfg.GetParallelism()
		sink.Properties = cfg.GetProperties()
	}

	for _, m := range rule.GetMapping() {
		sink.Mappings = append(sink.Mappings, parseMapping(m))
	}

	// 从 Proto Message 提取字段信息
	var protoFields map[string]ProtoFieldInfo
	var headerFields []MappingInfo
	var dataFields []MappingInfo
	if method != nil && method.Input != nil {
		protoFields = make(map[string]ProtoFieldInfo)
		for _, f := range method.Input.Fields {
			info := extractProtoField(f)
			protoFields[info.Name] = info

			// 构建 header/data 字段映射
			if info.IsHeader {
				headerFields = append(headerFields, MappingInfo{
					HeaderPath:   info.Name,
					HeaderName:   info.Name,
					TargetColumn: info.Name,
					Type:         protoTypeToColumnType(info.ProtoType, info.IsMap, info.IsMessage, info.IsEnum),
					NotNull:      !info.IsOptional,
					Comment:      info.Comment,
					IsHeader:     true,
				})
			} else {
				dataFields = append(dataFields, MappingInfo{
					DataPath:     "$." + info.Name,
					TargetColumn: info.Name,
					Type:         protoTypeToColumnType(info.ProtoType, info.IsMap, info.IsMessage, info.IsEnum),
					NotNull:      !info.IsOptional,
					Comment:      info.Comment,
					IsData:       true,
				})
			}
		}
	}

	// 自动推断 data_path 字段（保持向后兼容）
	if sink.AutoInferDataMapping && method != nil {
		inputMsg := method.Input
		if inputMsg != nil {
			sink.InferredFields = inferFieldsFromMessage(inputMsg)
		}
	}

	// auto_map: 自动映射所有字段
	if sink.AutoMap && method != nil && method.Input != nil {
		// 合并 header + data 字段
		autoMappings := append(headerFields, dataFields...)

		// 过滤 ignore 字段
		ignoreSet := make(map[string]bool)
		for _, m := range sink.Mappings {
			if m.Ignore {
				// 从 path 提取字段名
				path := m.DataPath
				if path == "" {
					path = m.HeaderPath
				}
				if len(path) > 2 && path[:2] == "$." {
					path = path[2:]
				}
				ignoreSet[path] = true
			}
		}
		var filtered []MappingInfo
		for _, m := range autoMappings {
			name := m.TargetColumn
			if m.IsData && len(m.DataPath) > 2 {
				name = m.DataPath[2:]
			}
			if !ignoreSet[name] {
				filtered = append(filtered, m)
			}
		}

		// 合并：显式 mapping 覆盖自动映射
		sink.Mappings = mergeAutoMappings(sink.Mappings, filtered)
	}

	// 为显式 mapping 补全注释和类型推断
	if protoFields != nil {
		for i := range sink.Mappings {
			m := &sink.Mappings[i]
			// 补全注释
			if m.Comment == "" && m.IsData {
				fieldName := strings.TrimPrefix(m.DataPath, "$.")
				if pf, ok := protoFields[fieldName]; ok && pf.Comment != "" {
					m.Comment = pf.Comment
				}
			}
			// 推断类型 (Strict Mode)
			if m.Type == "" || m.Type == "COLUMN_TYPE_UNSPECIFIED" {
				inferredType, err := inferType(*m, protoFields)
				if err != nil {
					return sink, fmt.Errorf("sink rule for method %s: %w", method.Desc.Name(), err)
				}
				m.Type = inferredType
			}
		}
	}

	return sink, nil
}

// inferFieldsFromMessage 从 Proto Message 自动推断 data_path 映射
func inferFieldsFromMessage(msg *protogen.Message) []MappingInfo {
	var fields []MappingInfo
	for _, f := range msg.Fields {
		info := extractProtoField(f)
		m := MappingInfo{
			DataPath:     "$." + info.Name,
			TargetColumn: info.Name,
			Type:         protoTypeToColumnType(info.ProtoType, info.IsMap, info.IsMessage, info.IsEnum),
			NotNull:      !info.IsOptional,
			Comment:      info.Comment,
			IsData:       true,
		}
		fields = append(fields, m)
	}
	return fields
}

// extractProtoField 从 protogen.Field 提取字段信息
func extractProtoField(f *protogen.Field) ProtoFieldInfo {
	info := ProtoFieldInfo{
		Name:       string(f.Desc.Name()),
		GoName:     f.GoName,
		IsOptional: f.Desc.HasOptionalKeyword(),
		IsMap:      f.Desc.IsMap(),
	}

	// 获取字段注释
	if f.Comments.Leading != "" {
		info.Comment = strings.TrimSpace(strings.TrimPrefix(string(f.Comments.Leading), "//"))
	}

	// 检查 is_event_header
	if f.Desc.Options() != nil {
		info.IsHeader = proto.GetExtension(f.Desc.Options(), eventapiv1.E_IsEventHeader).(bool)
	}

	// 确定 Proto 类型
	if f.Message != nil {
		msgName := string(f.Message.Desc.FullName())
		info.ProtoType = msgName
		info.IsMessage = true
	} else if f.Enum != nil {
		info.ProtoType = "enum"
		info.IsEnum = true
	} else {
		info.ProtoType = f.Desc.Kind().String()
	}

	return info
}

// protoTypeToColumnType 将 Proto 类型映射为 Sink ColumnType
func protoTypeToColumnType(protoType string, isMap bool, isMessage bool, isEnum bool) string {
	if isMap {
		return "COLUMN_TYPE_JSON"
	}
	if isEnum {
		return "COLUMN_TYPE_VARCHAR"
	}
	if isMessage {
		switch protoType {
		case "google.protobuf.Timestamp":
			return "COLUMN_TYPE_TIMESTAMP"
		case "google.protobuf.Struct", "google.protobuf.Value":
			return "COLUMN_TYPE_JSON"
		default:
			return "COLUMN_TYPE_JSON"
		}
	}
	switch protoType {
	case "string", "bytes":
		return "COLUMN_TYPE_VARCHAR"
	case "int32", "int64", "uint32", "uint64", "sint32", "sint64", "fixed32", "fixed64", "sfixed32", "sfixed64":
		return "COLUMN_TYPE_BIGINT"
	case "bool":
		return "COLUMN_TYPE_BOOLEAN"
	case "float", "double":
		return "COLUMN_TYPE_DOUBLE"
	default:
		return "COLUMN_TYPE_VARCHAR"
	}
}

func parseMapping(m *eventapiv1.FieldMapping) MappingInfo {
	info := MappingInfo{
		TargetColumn: m.GetTargetColumn(),
		Type:         m.GetType().String(),
		NotNull:      m.GetNotNull(),
		Comment:      m.GetComment(),
		Ignore:       m.GetIgnore(),
	}

	switch src := m.GetSource().(type) {
	case *eventapiv1.FieldMapping_HeaderPath:
		info.HeaderPath = src.HeaderPath
		info.IsHeader = true
		// 嵌套 JSON 提取: "$.ce_source.tenant"
		if len(src.HeaderPath) > 2 && src.HeaderPath[:2] == "$." {
			info.IsNestedJSON = true
			info.NestedJSONKey = src.HeaderPath
			// 提取 header 名称: "$.ce_source.tenant" -> "ce_source"
			parts := strings.SplitN(src.HeaderPath[2:], ".", 2)
			info.HeaderName = parts[0]
		} else {
			info.HeaderName = src.HeaderPath
		}
	case *eventapiv1.FieldMapping_DataPath:
		info.DataPath = src.DataPath
		info.IsData = true
	}

	return info
}

// mergeMappings 合并显式映射与自动推断字段（显式优先）
func mergeMappings(explicit []MappingInfo, inferred []MappingInfo) []MappingInfo {
	seen := make(map[string]bool)
	for _, m := range explicit {
		seen[m.TargetColumn] = true
	}

	result := make([]MappingInfo, len(explicit))
	copy(result, explicit)

	for _, m := range inferred {
		if !seen[m.TargetColumn] {
			result = append(result, m)
		}
	}
	return result
}

// mergeAutoMappings 合并显式映射与 auto_map 生成的字段
func mergeAutoMappings(explicit []MappingInfo, auto []MappingInfo) []MappingInfo {
	seen := make(map[string]bool)
	for _, m := range explicit {
		if !m.Ignore {
			seen[m.TargetColumn] = true
		}
	}

	result := make([]MappingInfo, 0, len(auto)+len(explicit))
	for _, m := range auto {
		if !seen[m.TargetColumn] {
			result = append(result, m)
		}
	}

	for _, m := range explicit {
		if !m.Ignore {
			result = append(result, m)
		}
	}

	return result
}
