package main

import (
	"testing"

	eventapiv1 "github.com/eventapi/eventapi-go/pkg/eventapi/v1"
)

func TestParseMappingHeaderPath(t *testing.T) {
	m := &eventapiv1.FieldMapping{
		Source:       &eventapiv1.FieldMapping_HeaderPath{HeaderPath: "ce_id"},
		TargetColumn: "event_id",
		Type:         eventapiv1.ColumnType_COLUMN_TYPE_VARCHAR,
		NotNull:      true,
		Comment:      "Event ID",
	}

	info := parseMapping(m)

	if !info.IsHeader {
		t.Error("expected IsHeader to be true")
	}
	if info.HeaderPath != "ce_id" {
		t.Errorf("HeaderPath = %s, want ce_id", info.HeaderPath)
	}
	if info.TargetColumn != "event_id" {
		t.Errorf("TargetColumn = %s, want event_id", info.TargetColumn)
	}
	if !info.NotNull {
		t.Error("expected NotNull to be true")
	}
	if info.Comment != "Event ID" {
		t.Errorf("Comment = %s, want 'Event ID'", info.Comment)
	}
}

func TestParseMappingDataPath(t *testing.T) {
	m := &eventapiv1.FieldMapping{
		Source:       &eventapiv1.FieldMapping_DataPath{DataPath: "$.user_id"},
		TargetColumn: "user_id",
		Type:         eventapiv1.ColumnType_COLUMN_TYPE_VARCHAR,
	}

	info := parseMapping(m)

	if !info.IsData {
		t.Error("expected IsData to be true")
	}
	if info.DataPath != "$.user_id" {
		t.Errorf("DataPath = %s, want $.user_id", info.DataPath)
	}
	if info.IsHeader {
		t.Error("expected IsHeader to be false for data path")
	}
}

func TestParseMappingNestedJSON(t *testing.T) {
	m := &eventapiv1.FieldMapping{
		Source:       &eventapiv1.FieldMapping_HeaderPath{HeaderPath: "$.ce_source.tenant"},
		TargetColumn: "tenant_id",
		Type:         eventapiv1.ColumnType_COLUMN_TYPE_VARCHAR,
	}

	info := parseMapping(m)

	if !info.IsNestedJSON {
		t.Error("expected IsNestedJSON to be true")
	}
	if info.NestedJSONKey != "$.ce_source.tenant" {
		t.Errorf("NestedJSONKey = %s, want $.ce_source.tenant", info.NestedJSONKey)
	}
}

func TestParseMappingIgnore(t *testing.T) {
	m := &eventapiv1.FieldMapping{
		Source:       &eventapiv1.FieldMapping_DataPath{DataPath: "$.password"},
		TargetColumn: "password",
		Ignore:       true,
	}

	info := parseMapping(m)

	if !info.Ignore {
		t.Error("expected Ignore to be true")
	}
}

func TestParseSinkRule(t *testing.T) {
	rule := &eventapiv1.SinkRule{
		Target:       eventapiv1.SinkType_SINK_TYPE_STARROCKS,
		Database:     "analytics",
		Table:        "user_events",
		PrimaryKey:   []string{"event_id"},
		PartitionKey: []string{"event_time"},
		SourceTopic:  "user.events",
		Mapping: []*eventapiv1.FieldMapping{
			{
				Source:       &eventapiv1.FieldMapping_HeaderPath{HeaderPath: "ce_id"},
				TargetColumn: "event_id",
				Type:         eventapiv1.ColumnType_COLUMN_TYPE_VARCHAR,
				NotNull:      true,
			},
			{
				Source:       &eventapiv1.FieldMapping_DataPath{DataPath: "$.user_id"},
				TargetColumn: "user_id",
				Type:         eventapiv1.ColumnType_COLUMN_TYPE_VARCHAR,
			},
		},
		Config: &eventapiv1.SinkConfig{
			CheckpointInterval: "60s",
			BatchSize:          1000,
			Parallelism:        4,
		},
	}

	sink, err := parseSinkRule(nil, rule, nil)
	if err != nil {
		t.Fatalf("parseSinkRule error: %v", err)
	}

	if sink.Target != "SINK_TYPE_STARROCKS" {
		t.Errorf("Target = %s, want SINK_TYPE_STARROCKS", sink.Target)
	}
	if sink.Database != "analytics" {
		t.Errorf("Database = %s, want analytics", sink.Database)
	}
	if sink.Table != "user_events" {
		t.Errorf("Table = %s, want user_events", sink.Table)
	}
	if len(sink.PrimaryKey) != 1 || sink.PrimaryKey[0] != "event_id" {
		t.Errorf("PrimaryKey = %v, want [event_id]", sink.PrimaryKey)
	}
	if len(sink.PartitionKey) != 1 || sink.PartitionKey[0] != "event_time" {
		t.Errorf("PartitionKey = %v, want [event_time]", sink.PartitionKey)
	}
	if sink.SourceTopic != "user.events" {
		t.Errorf("SourceTopic = %s, want user.events", sink.SourceTopic)
	}
	if sink.Checkpoint != "60s" {
		t.Errorf("Checkpoint = %s, want 60s", sink.Checkpoint)
	}
	if sink.BatchSize != 1000 {
		t.Errorf("BatchSize = %d, want 1000", sink.BatchSize)
	}
	if sink.Parallelism != 4 {
		t.Errorf("Parallelism = %d, want 4", sink.Parallelism)
	}
	if len(sink.Mappings) != 2 {
		t.Fatalf("Mappings count = %d, want 2", len(sink.Mappings))
	}
	if !sink.Mappings[0].IsHeader {
		t.Error("first mapping should be header")
	}
	if !sink.Mappings[1].IsData {
		t.Error("second mapping should be data")
	}
}

func TestParseSinkRuleEmpty(t *testing.T) {
	rule := &eventapiv1.SinkRule{}
	sink, err := parseSinkRule(nil, rule, nil)
	if err != nil {
		t.Fatalf("parseSinkRule error: %v", err)
	}

	if sink.Target != "SINK_TYPE_UNSPECIFIED" {
		t.Errorf("Target = %s, want SINK_TYPE_UNSPECIFIED", sink.Target)
	}
	if len(sink.Mappings) != 0 {
		t.Errorf("Mappings count = %d, want 0", len(sink.Mappings))
	}
}

func TestParseSinkRuleWithDataContentType(t *testing.T) {
	rule := &eventapiv1.SinkRule{
		Target:               eventapiv1.SinkType_SINK_TYPE_STARROCKS,
		Database:             "analytics",
		Table:                "user_events",
		EventDataContentType: eventapiv1.EventDataContentType_CONTENT_TYPE_JSON,
	}

	sink, err := parseSinkRule(nil, rule, nil)
	if err != nil {
		t.Fatalf("parseSinkRule error: %v", err)
	}

	if sink.EventDataContentType != "CONTENT_TYPE_JSON" {
		t.Errorf("EventDataContentType = %s, want CONTENT_TYPE_JSON", sink.EventDataContentType)
	}
}

func TestParseSinkRuleWithAutoInfer(t *testing.T) {
	val := true
	rule := &eventapiv1.SinkRule{
		Target:               eventapiv1.SinkType_SINK_TYPE_STARROCKS,
		Database:             "analytics",
		Table:                "user_events",
		AutoInferDataMapping: &val,
	}

	sink, err := parseSinkRule(nil, rule, nil)
	if err != nil {
		t.Fatalf("parseSinkRule error: %v", err)
	}

	if !sink.AutoInferDataMapping {
		t.Error("expected AutoInferDataMapping to be true")
	}
}

func TestParseSinkRuleWithAutoInferDisabled(t *testing.T) {
	val := false
	rule := &eventapiv1.SinkRule{
		Target:               eventapiv1.SinkType_SINK_TYPE_STARROCKS,
		AutoInferDataMapping: &val,
	}

	sink, err := parseSinkRule(nil, rule, nil)
	if err != nil {
		t.Fatalf("parseSinkRule error: %v", err)
	}

	if sink.AutoInferDataMapping {
		t.Error("expected AutoInferDataMapping to be false")
	}
}

func TestParseSinkRuleWithAutoMap(t *testing.T) {
	rule := &eventapiv1.SinkRule{
		Target:   eventapiv1.SinkType_SINK_TYPE_STARROCKS,
		Database: "test",
		Table:    "test_table",
		AutoMap:  true,
	}

	sink, err := parseSinkRule(nil, rule, nil)
	if err != nil {
		t.Fatalf("parseSinkRule error: %v", err)
	}

	if !sink.AutoMap {
		t.Error("expected AutoMap to be true")
	}
}

func TestInferTypeExplicit(t *testing.T) {
	m := MappingInfo{Type: "COLUMN_TYPE_BIGINT"}
	got, err := inferType(m, nil)
	if err != nil {
		t.Fatalf("inferType error: %v", err)
	}
	if got != "COLUMN_TYPE_BIGINT" {
		t.Errorf("inferType = %s, want COLUMN_TYPE_BIGINT", got)
	}
}

func TestInferTypeStandardHeader(t *testing.T) {
	tests := []struct {
		headerPath string
		want       string
	}{
		{"ce_id", "COLUMN_TYPE_VARCHAR"},
		{"ce_time", "COLUMN_TYPE_TIMESTAMP"},
		{"ce_type", "COLUMN_TYPE_VARCHAR"},
		{"ce_source", "COLUMN_TYPE_VARCHAR"},
	}

	for _, tt := range tests {
		t.Run(tt.headerPath, func(t *testing.T) {
			m := MappingInfo{
				HeaderPath: tt.headerPath,
				IsHeader:   true,
			}
			got, err := inferType(m, nil)
			if err != nil {
				t.Fatalf("inferType error: %v", err)
			}
			if got != tt.want {
				t.Errorf("inferType(%s) = %s, want %s", tt.headerPath, got, tt.want)
			}
		})
	}
}

func TestInferTypeUnknownHeaderError(t *testing.T) {
	m := MappingInfo{
		HeaderPath: "x-custom-trace",
		IsHeader:   true,
	}
	_, err := inferType(m, nil)
	if err == nil {
		t.Fatal("expected error for unknown header, got nil")
	}
}

func TestMergeAutoMappings(t *testing.T) {
	explicit := []MappingInfo{
		{HeaderPath: "ce_id", TargetColumn: "event_id", Type: "COLUMN_TYPE_VARCHAR", IsHeader: true},
		{DataPath: "$.password", TargetColumn: "password", Ignore: true},
	}
	auto := []MappingInfo{
		{DataPath: "$.user_id", TargetColumn: "user_id", Type: "COLUMN_TYPE_VARCHAR", IsData: true},
		{DataPath: "$.amount", TargetColumn: "amount", Type: "COLUMN_TYPE_BIGINT", IsData: true},
	}

	result := mergeAutoMappings(explicit, auto)

	// Should have: 1 explicit (non-ignore) + 2 auto = 3
	if len(result) != 3 {
		t.Fatalf("mergeAutoMappings() count = %d, want 3", len(result))
	}

	// Check that ignore field is excluded
	for _, m := range result {
		if m.TargetColumn == "password" {
			t.Error("ignored field 'password' should not be in result")
		}
	}

	// Check that explicit override is present
	found := false
	for _, m := range result {
		if m.TargetColumn == "event_id" {
			found = true
		}
	}
	if !found {
		t.Error("explicit override 'event_id' should be in result")
	}
}

func TestMergeAutoMappingsOverride(t *testing.T) {
	explicit := []MappingInfo{
		{DataPath: "$.user_id", TargetColumn: "custom_user_id", Type: "COLUMN_TYPE_VARCHAR", IsData: true},
	}
	auto := []MappingInfo{
		{DataPath: "$.user_id", TargetColumn: "user_id", Type: "COLUMN_TYPE_VARCHAR", IsData: true},
	}

	result := mergeAutoMappings(explicit, auto)

	if len(result) != 2 {
		t.Fatalf("mergeAutoMappings() count = %d, want 2", len(result))
	}
}

func TestInferTypeUnspecifiedError(t *testing.T) {
	m := MappingInfo{
		HeaderPath: "x-custom-trace",
		IsHeader:   true,
		Type:       "COLUMN_TYPE_UNSPECIFIED",
	}
	_, err := inferType(m, nil)
	if err == nil {
		t.Fatal("expected error for unspecified type with unknown header, got nil")
	}
}

func TestTemplateContentTypeJSON(t *testing.T) {
	// 验证 CONTENT_TYPE_JSON 生成 'format' = 'json'
	got := toSQLFormat("CONTENT_TYPE_JSON")
	if got != "json" {
		t.Errorf("toSQLFormat(CONTENT_TYPE_JSON) = %s, want json", got)
	}
}

func TestTemplateContentTypeRaw(t *testing.T) {
	// 验证非 JSON 格式生成 'format' = 'raw'
	got := toSQLFormat("CONTENT_TYPE_PROTOBUF")
	if got != "raw" {
		t.Errorf("toSQLFormat(CONTENT_TYPE_PROTOBUF) = %s, want raw", got)
	}
}

func TestTemplateContentTypeEmpty(t *testing.T) {
	// 验证空格式生成 'format' = 'raw'
	got := toSQLFormat("")
	if got != "raw" {
		t.Errorf("toSQLFormat(\"\") = %s, want raw", got)
	}
}
