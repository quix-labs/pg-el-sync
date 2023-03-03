package types

import (
	"errors"
	"go_pg_es_sync/internals/utils"
)

type SimpleField struct {
	Alias string
	Field string
}
type ScriptedField struct {
	Alias  string
	Script string
}
type Fields struct {
	Simple   []*SimpleField
	Scripted []*ScriptedField
}

func (fields *Fields) Len() int {
	return len(fields.Simple) + len(fields.Scripted)
}

func (fields *Fields) Parse(config any) error {
	var sliceFields []any
	err := utils.ParseMap(config, &sliceFields)
	if err != nil {
		return err
	}
	for _, field := range sliceFields {
		switch parsed := field.(type) {
		case string:
			fields.Simple = append(fields.Simple, &SimpleField{Alias: parsed, Field: parsed})

		case map[string]interface{}:
			var tempField struct {
				Alias  string
				Field  string
				Script string
			}
			err := utils.ParseMap(parsed, &tempField)
			if err != nil {
				return errors.New("unable to parse field")
			}
			if tempField.Alias != "" && tempField.Field != "" {
				fields.Simple = append(fields.Simple, &SimpleField{Alias: tempField.Alias, Field: tempField.Field})
				continue
			}

			if tempField.Alias != "" && tempField.Script != "" {
				fields.Scripted = append(fields.Scripted, &ScriptedField{Alias: tempField.Alias, Script: tempField.Script})
				continue
			}

			return errors.New("field is not correct alias or scripted fields")

		default:
			return errors.New("invalid field")
		}
	}
	return nil
}
