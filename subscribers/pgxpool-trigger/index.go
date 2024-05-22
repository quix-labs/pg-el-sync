package pgxpool_trigger

import (
	"fmt"
	"github.com/quix-labs/pg-el-sync/internals/types"
	"strings"
)

type Index types.Index

func (index *Index) GetSelectQuery() string {
	additionalFields := map[string]string{}
	var leftJoins []string

	for _, relation := range index.Relations {
		rel := Relation(*relation)

		leftJoins = append(leftJoins, rel.GetLeftJoinQuery(index.Table))
		additionalFields[relation.Name] = rel.GetLeftJoinField()
	}

	fields := Fields(index.Fields)
	query := fmt.Sprintf(
		`SELECT %s AS "result", "%s"."%s" AS "reference" FROM "%s" %s`,
		fields.asJsonBuildObjectQuery(index.Table, additionalFields),
		index.Table,
		index.ReferenceField,
		index.Table,
		strings.Join(leftJoins, " "),
	)
	return query
}

func (index *Index) GetWhereRelationQuery(relationUpdates types.RelationsUpdate) string {
	var relationSelects []string

	//Split pivot and direct column
	for relation, references := range relationUpdates {
		rel := Relation(*relation)
		if rel.Parent == nil {
			var referencesRaw []string
			for _, reference := range references {
				referencesRaw = append(referencesRaw, `'`+reference.Reference+`'`)
			}
			relationSelects = append(relationSelects, fmt.Sprintf(
				`"%s"."%s"::TEXT IN (%s)`,
				index.Table, rel.ForeignKey.Parent, strings.Join(referencesRaw, ","),
			))
		} else {
			relationSelects = append(relationSelects, "EXISTS ("+rel.GetReverseSelectQuery(index.Table, references, "")+")")
		}
	}
	return "WHERE ( " + strings.Join(relationSelects, " OR ") + ")"
}
