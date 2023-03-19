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
	Fields     Fields

	Wheres    Wheres
	Relations Relations
	Mappings  map[string]any

	Parent    *Relation
	UniqueKey string

	ForeignKey ForeignKey
	UniqueName string
}
type Relations map[string]*Relation

func (relations *Relations) Parse(config any, parent *Relation) error {
	*relations = make(map[string]*Relation)

	var tempRelations []map[string]any
	err := utils.ParseMap(config, &tempRelations)
	if err != nil {
		return errors.New("unable to parse relations")
	}

	for _, relation := range tempRelations {
		rel := &Relation{}
		err = rel.Parse(relation, parent)
		if err != nil {
			return err
		}
		if parent != nil {
			rel.Parent = parent
		}
		(*relations)[rel.UniqueName] = rel
	}
	return nil
}

func (relation *Relation) getUniqueName() string {
	name := relation.Name
	if relation.Parent != nil {
		name = relation.Parent.getUniqueName() + "_" + name
	}
	return name
}
func (relation *Relation) Parse(config any, parent *Relation) error {
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
	if _, exists := rel["mappings"]; exists {
		err = utils.ParseMapKey(rel, "mappings", &relation.Mappings)
		if err != nil {
			return errors.New("invalid table for mapping")
		}
	}
	if parent != nil {
		relation.Parent = parent
	}
	relation.UniqueName = relation.getUniqueName()
	return nil
}
func (relation *Relation) GetFullName() string {
	name := relation.Name
	if relation.Parent != nil {
		name = relation.Parent.GetFullName() + "." + name
	}
	return name
}
func (relation *Relation) GetAllRelations() Relations {
	relations := make(Relations)
	for relationName, rel := range relation.Relations {
		relations[relationName] = rel
		for subRelName, subRel := range rel.GetAllRelations() {
			relations[subRelName] = subRel
		}
	}
	return relations
}

func (relation *Relation) GetMapping() map[string]any {
	return relation.Mappings
}
