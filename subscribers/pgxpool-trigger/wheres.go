package pgxpool_trigger

import (
	"fmt"
	"github.com/quix-labs/pg-el-sync/internals/types"
	"strings"
)

type Wheres types.Wheres

func (wheres *Wheres) GetConditionSql(table string, stripQuote bool) string {
	var wheresSql []string
	for _, where := range *wheres {
		if stripQuote {
			wheresSql = append(wheresSql, fmt.Sprintf(`%s."%s" %s`, table, where.Column, where.Condition))
		} else {
			wheresSql = append(wheresSql, fmt.Sprintf(`"%s"."%s" %s`, table, where.Column, where.Condition))
		}
	}
	return strings.Join(wheresSql, " AND ")
}

func (wheres *Wheres) GetWhereSql(table string) string {
	sql := wheres.GetConditionSql(table, false)
	if sql != "" {
		return "WHERE (" + sql + ")"
	}
	return ""
}
