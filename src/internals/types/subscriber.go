package types

type AbstractSubscriber interface {
	Init(config map[string]any)

	PrepareListen(indices []Index)
	Listen()
	Terminate()

	InternalInit(eventChannel *chan *interface{}, name string)
	InternalTerminate()
	DispatchEvent(event *interface{})

	GetAllRecordsForIndex(index *Index) <-chan Record
	GetFullRecordsForIndex(references []string, index *Index) <-chan Record
	GetFullRecordsForRelationUpdate(results RelationsUpdate, index *Index) <-chan Record
}

type Record struct {
	Reference string
	Data      map[string]interface{}
}
