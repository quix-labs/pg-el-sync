package postgresql

import (
	"fmt"
	"go_pg_es_sync/internals/types"
	"strings"
)

type Fields types.Fields

func (fields *Fields) asJsonBuildObjectQuery(table string, additional map[string]string) string {
	var raw []string

	for _, field := range fields.Simple {
		raw = append(raw, fmt.Sprintf(`'%s',"%s"."%s"`, field.Alias, table, field.Field))
	}
	for _, field := range fields.Scripted {
		raw = append(raw, fmt.Sprintf(`'%s',%s`, field.Alias, field.Script))
	}
	for alias, field := range additional {
		raw = append(raw, fmt.Sprintf(`'%s',%s`, alias, field))
	}

	return "JSON_BUILD_OBJECT(" + strings.Join(raw, ",") + ")"
}
