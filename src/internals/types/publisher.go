package types

type AbstractPublisher interface {
	Init(config map[string]any, Indices []*Index)
	Terminate()

	InternalInit(name string)
	InternalTerminate()
	Insert(rows []*InsertsRow)
	Update(rows []*UpdateRow)
	Delete(rows []*DeleteRow)
}

type InsertsRow struct {
	Index     string
	Reference string
	Record    map[string]interface{}
}

type UpdateRow struct {
	Index     string
	Reference string
	Record    map[string]interface{}
}

type DeleteRow struct {
	Index     string
	Reference string
}
