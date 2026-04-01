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

var tpl *template.Template

func init() {
	funcMap := template.FuncMap{
		"sqlType":       toSQLType,
		"sourceExpr":    toSourceExpr,
		"headerName":    toHeaderName,
		"uniqueHeaders": uniqueHeaders,
		"cleanTarget":   cleanTarget,
		"dataFieldsSQL": dataFieldsSQL,
		"lower":         strings.ToLower,
		"capitalize":    capitalize,
		"lastIndex":     func(n int) int { return n - 1 },
	}
	tpl = template.Must(template.New("").Funcs(funcMap).ParseFS(templatesFS, "templates/*.tmpl"))
}

func generateSink(gen *protogen.Plugin, file *protogen.File, target string) error {
	sinks, err := ParseSinks(file)
	if err != nil {
		return err
	}
	if len(sinks) == 0 {
		return nil
	}

	for _, sink := range sinks {
		data := map[string]any{
			"Sink":     sink,
			"Mappings": sink.Mappings,
		}

		// DDL
		ddlFile := gen.NewGeneratedFile(
			fmt.Sprintf("%s_ddl.sql", sink.Table),
			file.GoImportPath,
		)
		if err := tpl.ExecuteTemplate(ddlFile, "ddl.tmpl", data); err != nil {
			return fmt.Errorf("render ddl: %w", err)
		}

		// Flink SQL
		flinkFile := gen.NewGeneratedFile(
			fmt.Sprintf("%s_flink.sql", sink.Table),
			file.GoImportPath,
		)
		if err := tpl.ExecuteTemplate(flinkFile, "flink.tmpl", data); err != nil {
			return fmt.Errorf("render flink: %w", err)
		}
	}

	return nil
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func toSQLType(colType string, target string) string {
	switch colType {
	case "COLUMN_TYPE_VARCHAR":
		return "VARCHAR(256)"
	case "COLUMN_TYPE_BIGINT":
		return "BIGINT"
	case "COLUMN_TYPE_TIMESTAMP":
		if target == "CLICKHOUSE" {
			return "DateTime"
		}
		return "DATETIME"
	case "COLUMN_TYPE_DECIMAL":
		return "DECIMAL(10,2)"
	case "COLUMN_TYPE_BOOLEAN":
		return "BOOLEAN"
	case "COLUMN_TYPE_JSON":
		if target == "CLICKHOUSE" {
			return "String"
		}
		return "JSON"
	case "COLUMN_TYPE_DOUBLE":
		return "DOUBLE"
	default:
		return "VARCHAR(256)"
	}
}

func toSourceExpr(m MappingInfo, dataContentType string) string {
	switch {
	case m.IsNestedJSON:
		parts := strings.SplitN(m.NestedJSONKey, ".", 3)
		if len(parts) == 3 {
			return fmt.Sprintf("JSON_VALUE(%s, '$.%s')", parts[1], parts[2])
		}
		return m.NestedJSONKey
	case m.IsData:
		// JSON 模式下 Flink 已自动反序列化，直接引用列
		if dataContentType == "CONTENT_TYPE_JSON" {
			return m.TargetColumn
		}
		return fmt.Sprintf("JSON_VALUE(`value`, '%s')", m.DataPath)
	default:
		return m.HeaderPath
	}
}

func toHeaderName(headerPath string) string {
	return strings.TrimPrefix(headerPath, "ce_")
}

func uniqueHeaders(mappings []MappingInfo) []string {
	seen := make(map[string]bool)
	var result []string
	for _, m := range mappings {
		if m.IsHeader && !seen[m.HeaderName] {
			seen[m.HeaderName] = true
			result = append(result, m.HeaderName)
		}
	}
	return result
}

// cleanTarget extracts the clean target name: "SINK_TYPE_STARROCKS" -> "STARROCKS"
func cleanTarget(target string) string {
	return strings.TrimPrefix(target, "SINK_TYPE_")
}

// toSQLFormat returns the Flink SQL format based on content type
func toSQLFormat(contentType string) string {
	if contentType == "CONTENT_TYPE_JSON" {
		return "json"
	}
	return "raw"
}

// dataFieldsSQL generates the SQL column definitions for data fields with proper comma handling
func dataFieldsSQL(mappings []MappingInfo, target string) string {
	var fields []MappingInfo
	for _, m := range mappings {
		if m.IsData {
			fields = append(fields, m)
		}
	}
	if len(fields) == 0 {
		return ""
	}
	var parts []string
	for i, m := range fields {
		sql := fmt.Sprintf("  %s %s", m.TargetColumn, toSQLType(m.Type, target))
		if m.NotNull {
			sql += " NOT NULL"
		}
		if i < len(fields)-1 {
			sql += ","
		}
		parts = append(parts, sql)
	}
	return "\n" + strings.Join(parts, "\n") + "\n"
}
