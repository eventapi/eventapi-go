package main

import (
	"testing"
)

func TestProtoTypeToColumnType(t *testing.T) {
	tests := []struct {
		name      string
		protoType string
		isMap     bool
		isMessage bool
		isEnum    bool
		want      string
	}{
		{"string", "string", false, false, false, "COLUMN_TYPE_VARCHAR"},
		{"bytes", "bytes", false, false, false, "COLUMN_TYPE_VARCHAR"},
		{"int32", "int32", false, false, false, "COLUMN_TYPE_BIGINT"},
		{"int64", "int64", false, false, false, "COLUMN_TYPE_BIGINT"},
		{"uint32", "uint32", false, false, false, "COLUMN_TYPE_BIGINT"},
		{"bool", "bool", false, false, false, "COLUMN_TYPE_BOOLEAN"},
		{"float", "float", false, false, false, "COLUMN_TYPE_DOUBLE"},
		{"double", "double", false, false, false, "COLUMN_TYPE_DOUBLE"},
		{"Timestamp", "google.protobuf.Timestamp", false, true, false, "COLUMN_TYPE_TIMESTAMP"},
		{"Struct", "google.protobuf.Struct", false, true, false, "COLUMN_TYPE_JSON"},
		{"Value", "google.protobuf.Value", false, true, false, "COLUMN_TYPE_JSON"},
		{"other message", "some.other.Message", false, true, false, "COLUMN_TYPE_JSON"},
		{"map", "string", true, false, false, "COLUMN_TYPE_JSON"},
		{"enum", "enum", false, false, true, "COLUMN_TYPE_VARCHAR"},
		{"unknown", "unknown", false, false, false, "COLUMN_TYPE_VARCHAR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := protoTypeToColumnType(tt.protoType, tt.isMap, tt.isMessage, tt.isEnum)
			if got != tt.want {
				t.Errorf("protoTypeToColumnType(%s, map=%v, msg=%v, enum=%v) = %s, want %s",
					tt.protoType, tt.isMap, tt.isMessage, tt.isEnum, got, tt.want)
			}
		})
	}
}

func TestMergeMappings(t *testing.T) {
	explicit := []MappingInfo{
		{DataPath: "$.user_id", TargetColumn: "user_id", Type: "COLUMN_TYPE_VARCHAR", IsData: true},
		{HeaderPath: "ce_id", HeaderName: "ce_id", TargetColumn: "event_id", Type: "COLUMN_TYPE_VARCHAR", IsHeader: true},
	}
	inferred := []MappingInfo{
		{DataPath: "$.event_id", TargetColumn: "event_id", Type: "COLUMN_TYPE_VARCHAR", IsData: true},
		{DataPath: "$.email", TargetColumn: "email", Type: "COLUMN_TYPE_VARCHAR", IsData: true},
		{DataPath: "$.role", TargetColumn: "role", Type: "COLUMN_TYPE_VARCHAR", IsData: true},
	}

	result := mergeMappings(explicit, inferred)

	// Should have: 2 explicit + 2 inferred (event_id deduplicated by target_column)
	if len(result) != 4 {
		t.Fatalf("mergeMappings() count = %d, want 4", len(result))
	}

	// Check explicit mappings preserved
	if result[0].DataPath != "$.user_id" {
		t.Errorf("first mapping DataPath = %s, want $.user_id", result[0].DataPath)
	}
	if result[1].HeaderPath != "ce_id" {
		t.Errorf("second mapping HeaderPath = %s, want ce_id", result[1].HeaderPath)
	}

	// Check inferred mappings added (excluding event_id which has explicit target_column)
	dataCount := 0
	for _, m := range result {
		if m.IsData {
			dataCount++
		}
	}
	if dataCount != 3 {
		t.Errorf("data field count = %d, want 3", dataCount)
	}
}

func TestMergeMappingsNoExplicitData(t *testing.T) {
	explicit := []MappingInfo{
		{HeaderPath: "ce_id", HeaderName: "ce_id", TargetColumn: "event_id", Type: "COLUMN_TYPE_VARCHAR", IsHeader: true},
	}
	inferred := []MappingInfo{
		{DataPath: "$.user_id", TargetColumn: "user_id", Type: "COLUMN_TYPE_VARCHAR", IsData: true},
		{DataPath: "$.email", TargetColumn: "email", Type: "COLUMN_TYPE_VARCHAR", IsData: true},
	}

	result := mergeMappings(explicit, inferred)

	// Should have: 1 explicit header + 2 inferred data
	if len(result) != 3 {
		t.Fatalf("mergeMappings() count = %d, want 3", len(result))
	}
}

func TestMergeMappingsEmptyInferred(t *testing.T) {
	explicit := []MappingInfo{
		{DataPath: "$.user_id", TargetColumn: "user_id", Type: "COLUMN_TYPE_VARCHAR", IsData: true},
	}
	inferred := []MappingInfo{}

	result := mergeMappings(explicit, inferred)

	if len(result) != 1 {
		t.Fatalf("mergeMappings() count = %d, want 1", len(result))
	}
}
