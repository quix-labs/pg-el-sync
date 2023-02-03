package postgresql

import (
	"fmt"
	"go_pg_es_sync/internals/types"
	"strings"
)

type Relation types.Relation

func (rel *Relation) GetLeftJoinQuery(parentTable string) string {
	additionalFields := map[string]string{}
	var leftJoins []string

	for _, relation := range rel.Relations {
		subrel := Relation(*relation)
		additionalFields[relation.Name] = subrel.GetLeftJoinField()
		leftJoins = append(leftJoins, subrel.GetLeftJoinQuery(rel.Table))
	}

	fields := Fields(rel.Fields)

	switch rel.Type {
	case "many_to_many":
		return fmt.Sprintf(
			`LEFT OUTER JOIN (SELECT JSON_AGG(%s) AS "result", "%s"."%s" AS parent_ref FROM "%s" %s GROUP BY "%s"."%s") AS "%s" ON "%s"."parent_ref" = "%s"."%s"`,
			fields.asJsonBuildObjectQuery(rel.Table, additionalFields),
			rel.Table,
			rel.ForeignKey.Local,
			rel.Table,
			strings.Join(leftJoins, " "),
			rel.Table,
			rel.ForeignKey.Local,
			rel.Name,
			rel.Name,
			parentTable,
			rel.ForeignKey.Parent,
		)
	case "one_to_one":
		return fmt.Sprintf(
			`LEFT OUTER JOIN (SELECT %s AS "result", "%s"."%s" AS parent_ref FROM "%s" %s ) AS "%s" ON "%s"."parent_ref" = "%s"."%s"`,
			fields.asJsonBuildObjectQuery(rel.Table, additionalFields),
			rel.Table,
			rel.ForeignKey.Local,
			rel.Table,
			strings.Join(leftJoins, " "),
			rel.Name,
			rel.Name,
			parentTable,
			rel.ForeignKey.Parent,
		)
	}
	return ""
}

func (rel *Relation) GetLeftJoinField() string {
	return fmt.Sprintf(`"%s"."result"`, rel.Name)
}

func (rel *Relation) GetReverseSelectQuery(table string, references []string, subExists string) string {

	andRaw := ""
	if references != nil && len(references) > 0 {
		andRaw = `AND "id" IN (` + strings.Join(references, ", ") + `)`
	}
	fromTable := table
	if rel.Parent != nil {
		fromTable = rel.Parent.Table
	}
	selectSql := fmt.Sprintf(
		`SELECT * FROM "%s" WHERE "%s"."%s" = "%s"."%s" %s`,
		rel.Table,
		fromTable,
		rel.ForeignKey.Parent,
		rel.Table,
		rel.ForeignKey.Local,
		andRaw,
	)
	if subExists != "" {
		selectSql += `AND EXISTS (` + subExists + `)`
	}

	if rel.Parent != nil {
		parent := Relation(*rel.Parent)
		selectSql = parent.GetReverseSelectQuery(table, nil, selectSql)
	}

	return selectSql
}
