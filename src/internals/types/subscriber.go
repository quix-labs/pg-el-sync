package types

type AbstractSubscriber interface {
	Init(config map[string]any, Indices []Index)
	Listen()
	Terminate()

	InternalInit(*chan *interface{}, string)
	InternalTerminate()
	DispatchEvent(event *interface{})

	GetAllRecordsForIndex(index *Index) <-chan Record
	GetFullRecordsForIndex(references []string, index *Index) (map[string]map[string]interface{}, error)
	GetFullRecordsForRelationUpdate(results RelationsUpdate, index *Index) (map[string]map[string]interface{}, error) //@TODO Yield by channel function
}

type Record struct {
	Reference string
	Data      map[string]interface{}
}
