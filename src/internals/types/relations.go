package types

import (
	"errors"
	"go_pg_es_sync/internals/utils"
)

type ForeignKey struct {
	Local        string
	Parent       string
	PivotLocal   string `json:"pivot_local"`
	PivotTable   string `json:"pivot_table"`
	PivotFields  Fields
	PivotRelated string `json:"pivot_related"`
}
type Relation struct {
	Type       string // 'one_to_one', 'one_to_many', 'many_to_many'
	Name       string
	Table      string
	SoftDelete bool

	Fields Fields
	Wheres Wheres

	Relations Relations
	Parent    *Relation

	UniqueKey  string
	ForeignKey ForeignKey
}
type Relations []*Relation

func (relations *Relations) Parse(config any, parent *Relation) error {
	var tempRelations []map[string]any
	err := utils.ParseMap(config, &tempRelations)
	if err != nil {
		return errors.New("unable to parse relations")
	}

	for _, relation := range tempRelations {
		rel := &Relation{}
		err = rel.Parse(relation)
		if err != nil {
			return err
		}
		if parent != nil {
			rel.Parent = parent
		}
		*relations = append(*relations, rel)
	}
	return nil
}

func (relation *Relation) Parse(config any) error {
	var rel map[string]any
	err := utils.ParseMap(config, &rel)
	if err != nil {
		return errors.New("unable to parse relation")
	}
	err = utils.ParseMapKey(rel, "type", &relation.Type)
	if err != nil {
		return errors.New("type name for relation")
	}
	err = utils.ParseMapKey(rel, "name", &relation.Name)
	if err != nil {
		return errors.New("invalid name for relation")
	}
	err = utils.ParseMapKey(rel, "table", &relation.Table)
	if err != nil {
		return err
	}
	err = utils.ParseMapKey(rel, "soft_delete", &relation.SoftDelete)
	if err != nil {
		relation.SoftDelete = false
	}
	if _, exists := rel["foreign_key"]; !exists {
		return errors.New("you need to define foreign_key on relations")
	}

	err = utils.ParseMap(rel["foreign_key"], &relation.ForeignKey)
	if err != nil || relation.ForeignKey.Local == "" || relation.ForeignKey.Parent == "" {
		return errors.New("invalid relation foreign keys")
	}
	if relation.Type == "many_to_many" {
		if fields, exists := rel["foreign_key"].(map[string]any)["pivot_fields"]; exists {
			err = relation.ForeignKey.PivotFields.Parse(fields)
			if err != nil {
				return err
			}
		}
	}
	if _, exists := rel["fields"]; exists {
		err = relation.Fields.Parse(rel["fields"])
		if err != nil {
			return err
		}
	}
	if _, exists := rel["wheres"]; exists {
		err = relation.Wheres.Parse(rel["wheres"])
		if err != nil {
			return err
		}
	}
	if _, exists := rel["relations"]; exists {
		err = relation.Relations.Parse(rel["relations"], relation)
		if err != nil {
			return errors.New("invalid table for mapping")
		}
	}
	return nil
}

func (relation *Relation) GetAllRelations() []*Relation {
	var relations []*Relation
	for _, rel := range relation.Relations {
		relations = append(relations, rel)
		for _, subRel := range rel.GetAllRelations() {
			relations = append(relations, subRel)
		}
	}
	return relations
}

func (relation *Relation) GetDependsRelations(table string) []*Relation {
	var dependsRelations []*Relation
	if (relation.Table) == table {
		dependsRelations = append(dependsRelations, relation)
	}
	for _, rel := range relation.Relations {
		if rel.DependsOnTable(table) {
			dependsRelations = append(dependsRelations, rel.GetDependsRelations(table)...)
		}
	}
	return dependsRelations
}

func (relation *Relation) DependsOnTable(table string) bool {
	if relation.Table == table || relation.ForeignKey.PivotTable == table {
		return true
	}
	for _, rel := range relation.GetAllRelations() {
		if rel.DependsOnTable(table) {
			return true
		}
	}
	return false
}
