package postgresql

import (
	"fmt"
	"go_pg_es_sync/internals/types"
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
		relationSelects = append(relationSelects, rel.GetReverseSelectQuery(index.Table, references, ""))
	}

	query := "WHERE ( "
	for idx, relationSelect := range relationSelects {
		if idx == 0 {
			query += " EXISTS (" + relationSelect + ")"
		} else {
			query += " OR EXISTS (" + relationSelect + ")"
		}
	}
	query += ")"
	return query
}
