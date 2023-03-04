package types

import "go_pg_es_sync/internals/utils"

type InsertEvent struct {
	Index     string
	Reference string
}

type UpdateEvent struct {
	Index                 string
	Reference             string
	SoftDeleted           bool
	PreviouslySoftDeleted bool
}

type RelationUpdateEvent struct {
	Index     string
	Relation  string
	Reference string
	Pivot     bool
}

type DeleteEvent struct {
	Index     string
	Reference string
}

type WaitingEvents struct {
	Insert          utils.ConcurrentSlice[*InsertEvent]
	Update          utils.ConcurrentSlice[*UpdateEvent]
	Delete          utils.ConcurrentSlice[*DeleteEvent]
	RelationsUpdate utils.ConcurrentSlice[*RelationUpdateEvent]
}
