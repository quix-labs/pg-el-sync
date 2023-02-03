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
		`SELECT %s AS "result", "%s"."id" AS "reference" FROM "%s" %s`,
		fields.asJsonBuildObjectQuery(index.Table, additionalFields),
		index.Table,
		index.Table,
		strings.Join(leftJoins, " "),
	)
	return query
}

func (index *Index) GetWhereRelationQuery(relationUpdates types.RelationsUpdate) string {
	query := ""
	var relationSelects []string

	for relation, references := range relationUpdates {
		rel := Relation(*relation)
		relationSelects = append(relationSelects, rel.GetReverseSelectQuery(index.Table, references, ""))
	}

	for idx, relationSelect := range relationSelects {
		if idx == 0 {
			query += "WHERE EXISTS(" + relationSelect + ")"
		} else {
			query += " OR EXISTS(" + relationSelect + ")"
		}
	}

	return query
}
