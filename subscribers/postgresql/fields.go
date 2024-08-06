package postgresql

import (
	"fmt"
	"github.com/quix-labs/pg-el-sync/internals/types"
	"strings"
)

type Fields types.Fields

func (fields *Fields) asJsonBuildObjectQuery(table string, additional map[string]string) string {
	parsedFields := fields.getParsedFields(table, additional)
	var rawFields []string
	for alias, raw := range parsedFields {
		rawFields = append(rawFields, fmt.Sprintf("'%s',%s", alias, raw))
	}

	// Split into chunks of 50
	const chunkSize = 50

	// Prevent ERROR: cannot pass more than 100 arguments to a function (SQLSTATE 54023) using chunk
	if len(rawFields) <= chunkSize {
		return "JSON_BUILD_OBJECT(" + strings.Join(rawFields, ",") + ")"
	}

	var chunks []string
	for i := 0; i < len(rawFields); i += chunkSize {
		end := i + chunkSize
		if end > len(rawFields) {
			end = len(rawFields)
		}
		chunks = append(chunks, "JSONB_BUILD_OBJECT("+strings.Join(rawFields[i:end], ",")+")")
	}

	return strings.Join(chunks, " || ")
}

func (fields *Fields) getParsedFields(table string, additional map[string]string) map[string]string {
	raw := map[string]string{}
	if additional != nil {
		raw = additional
	}

	for _, field := range fields.Simple {
		raw[field.Alias] = fmt.Sprintf(`"%s"."%s"`, table, field.Field)
	}
	for _, field := range fields.Scripted {
		raw[field.Alias] = strings.ReplaceAll(field.Script, "{{table}}", table)
	}

	return raw
}
