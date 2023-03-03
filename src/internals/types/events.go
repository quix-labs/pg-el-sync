package types

import "go_pg_es_sync/internals/utils"

type InsertEvent struct {
	Table     string
	Reference string
}

type UpdateEvent struct {
	Table                 string
	Reference             string
	SoftDeleted           bool
	PreviouslySoftDeleted bool
}

type RelationUpdateEvent struct {
	Table     string
	Reference string
}

type DeleteEvent struct {
	Table     string
	Reference string
}

type WaitingEvents struct {
	Insert          utils.ConcurrentSlice[*InsertEvent]
	Update          utils.ConcurrentSlice[*UpdateEvent]
	Delete          utils.ConcurrentSlice[*DeleteEvent]
	RelationsUpdate utils.ConcurrentSlice[*RelationUpdateEvent]
}
