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

	softDeleteWhere := ""
	if rel.SoftDelete {
		softDeleteWhere = fmt.Sprintf(`WHERE "%s"."%s" IS NULL`, rel.Table, "deleted_at")
	}
	fields := Fields(rel.Fields)
	switch rel.Type {
	case "one_to_many":
		return fmt.Sprintf(
			`LEFT OUTER JOIN (SELECT JSON_AGG(%s) AS "result", "%s"."%s" AS parent_ref FROM "%s" %s %s GROUP BY "%s"."%s") AS "%s" ON "%s"."parent_ref" = "%s"."%s"`,
			fields.asJsonBuildObjectQuery(rel.Table, additionalFields),
			rel.Table,
			rel.ForeignKey.Local,
			rel.Table,
			strings.Join(leftJoins, " "),
			softDeleteWhere,
			rel.Table,
			rel.ForeignKey.Local,
			rel.Name,
			rel.Name,
			parentTable,
			rel.ForeignKey.Parent,
		)
	case "one_to_one":
		return fmt.Sprintf(
			`LEFT OUTER JOIN (SELECT %s AS "result", "%s"."%s" AS parent_ref FROM "%s" %s %s ) AS "%s" ON "%s"."parent_ref" = "%s"."%s"`,
			fields.asJsonBuildObjectQuery(rel.Table, additionalFields),
			rel.Table,
			rel.ForeignKey.Local,
			rel.Table,
			strings.Join(leftJoins, " "),
			softDeleteWhere,
			rel.Name,
			rel.Name,
			parentTable,
			rel.ForeignKey.Parent,
		)
	case "many_to_many":
		if rel.ForeignKey.PivotFields.Len() > 0 {
			pivotFields := Fields(rel.ForeignKey.PivotFields)
			for alias, raw := range pivotFields.getParsedFields(rel.ForeignKey.PivotTable, nil) {
				additionalFields[alias] = raw
			}
		}
		return fmt.Sprintf(
			`LEFT OUTER JOIN (SELECT JSON_AGG(%s) AS "result", "%s"."%s" AS parent_ref %s FROM "%s" INNER JOIN "%s" ON "%s"."%s"="%s"."%s" %s GROUP BY "%s"."%s") AS "%s" ON "%s"."parent_ref" = "%s"."%s"`,
			fields.asJsonBuildObjectQuery(rel.Table, additionalFields),
			rel.ForeignKey.PivotTable,
			rel.ForeignKey.PivotLocal,
			softDeleteWhere,
			rel.ForeignKey.PivotTable,
			rel.Table,
			rel.Table,
			rel.ForeignKey.Parent,
			rel.ForeignKey.PivotTable,
			rel.ForeignKey.PivotRelated,
			strings.Join(leftJoins, " "),
			rel.ForeignKey.PivotTable,
			rel.ForeignKey.PivotLocal,
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

func (rel *Relation) GetReverseSelectQuery(table string, events []*types.RelationUpdateEvent, subExists string) string {

	andRaw := ""
	if events != nil && len(events) > 0 {
		var pivotRelatedReferences []string
		var relatedReferences []string
		for _, event := range events {
			if event.Pivot {
				pivotRelatedReferences = append(pivotRelatedReferences, event.Reference)
			} else {
				relatedReferences = append(relatedReferences, event.Reference)
			}
		}
		var wheres []string
		if len(relatedReferences) > 0 {
			wheres = append(wheres, fmt.Sprintf(`"%s"."%s" IN (%s)`,
				rel.Table,
				rel.ForeignKey.Local,
				strings.Join(relatedReferences, ","),
			))
		}
		if len(pivotRelatedReferences) > 0 {
			wheres = append(wheres, fmt.Sprintf(`"%s"."%s" IN (%s)`,
				rel.ForeignKey.PivotTable,
				rel.ForeignKey.PivotLocal,
				strings.Join(pivotRelatedReferences, ","),
			))
		}
		andRaw = `AND (` + strings.Join(wheres, " OR ") + ")"
	}
	fromTable := table
	if rel.Parent != nil {
		fromTable = rel.Parent.Table
	}
	var selectSql string
	switch rel.Type {
	case "one_to_many", "one_to_one":
		selectSql = fmt.Sprintf(
			`SELECT * FROM "%s" WHERE "%s"."%s" = "%s"."%s" %s`,
			rel.Table,
			fromTable,
			rel.ForeignKey.Parent,
			rel.Table,
			rel.ForeignKey.Local,
			andRaw,
		)
	case "many_to_many":
		selectSql = fmt.Sprintf(
			`SELECT * FROM "%s" INNER JOIN "%s" ON "%s"."%s"="%s"."%s" WHERE "%s"."%s" = "%s"."%s" %s`,
			rel.Table,
			rel.ForeignKey.PivotTable,
			rel.Table,
			rel.ForeignKey.Local,
			rel.ForeignKey.PivotTable,
			rel.ForeignKey.PivotRelated,
			fromTable,
			rel.ForeignKey.Parent,
			rel.ForeignKey.PivotTable,
			rel.ForeignKey.PivotLocal,
			andRaw,
		)
	}
	if subExists != "" {
		selectSql += `AND EXISTS (` + subExists + `)`
	}

	if rel.Parent != nil {
		parent := Relation(*rel.Parent)
		selectSql = parent.GetReverseSelectQuery(table, nil, selectSql)
	}

	return selectSql
}
