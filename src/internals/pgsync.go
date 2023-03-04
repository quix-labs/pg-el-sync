package internals

import (
	"fmt"
	"go_pg_es_sync/internals/types"
	"go_pg_es_sync/internals/utils"
	"go_pg_es_sync/publishers/elastic"
	"go_pg_es_sync/subscribers/postgresql"
	"time"
)

type PgSync struct {
	config       *Config
	subscribers  map[string]types.AbstractSubscriber
	publishers   map[string]types.AbstractPublisher
	indices      map[string]*types.Index
	eventChannel chan *interface{}
}

func (pgSync *PgSync) Init(config *Config) error {
	pgSync.config = config
	err := pgSync.loadSubscribers()
	if err != nil {
		return err
	}
	err = pgSync.loadPublishers()
	if err != nil {
		return err
	}
	err = pgSync.loadIndices()
	if err != nil {
		return err
	}
	pgSync.eventChannel = make(chan *interface{}, 100)
	err = pgSync.initSubscribers()
	if err != nil {
		return err
	}
	err = pgSync.initPublishers()
	if err != nil {
		return err
	}
	return nil
}

func (pgSync *PgSync) Start() {
	for _, subscriber := range pgSync.GetSubscribers() {
		subscriber.PrepareListen(pgSync.getIndicesForSubscriber(subscriber))
		go subscriber.Listen()
	}

	for true {
		notification := <-pgSync.eventChannel
		switch event := (*notification).(type) {
		case types.DeleteEvent:
			pgSync.indices[event.Index].WaitingEvents.Delete.Append(&event)
		case types.InsertEvent:
			pgSync.indices[event.Index].WaitingEvents.Insert.Append(&event)
		case types.UpdateEvent:
			index := pgSync.indices[event.Index]
			/**----SOFT DELETE------*/
			if event.SoftDeleted && !event.PreviouslySoftDeleted {
				index.WaitingEvents.Delete.Append(&types.DeleteEvent{
					Index:     event.Index,
					Reference: event.Reference,
				})
				continue
			}
			if !event.SoftDeleted && event.PreviouslySoftDeleted {
				index.WaitingEvents.Insert.Append(&types.InsertEvent{
					Index:     event.Index,
					Reference: event.Reference,
				})
				continue
			}
			index.WaitingEvents.Update.Append(&event)

		case types.RelationUpdateEvent:
			pgSync.indices[event.Index].WaitingEvents.RelationsUpdate.Append(&event)
		}
	}
}

func (pgSync *PgSync) GetSubscribers() map[string]types.AbstractSubscriber {
	return pgSync.subscribers
}

func (pgSync *PgSync) GetSubscriber(name string) (types.AbstractSubscriber, error) {
	subscriber, ok := pgSync.subscribers[name]
	if !ok {
		return nil, fmt.Errorf("invalid subscriber name: %s", name)
	}
	return subscriber, nil
}

func (pgSync *PgSync) GetPublishers() map[string]types.AbstractPublisher {
	return pgSync.publishers
}

func (pgSync *PgSync) GetPublisher(name string) (types.AbstractPublisher, error) {
	publisher, ok := pgSync.publishers[name]
	if !ok {
		return nil, fmt.Errorf("invalid publisher name: %s", name)
	}
	return publisher, nil
}
func (pgSync *PgSync) FullReindex() {
	i := len(pgSync.indices)
	finishedChan := make(chan *types.Index)

	start := time.Now()
	for _, index := range pgSync.indices {
		index := index
		go func() {
			index.IndexAllDocuments()
			finishedChan <- index
		}()
	}
	for i > 0 {
		idx := <-finishedChan
		i--
		end := time.Since(start).String()
		fmt.Printf("Indexing %s finished in %s!\n", idx.Name, end)
	}
}

// -----------------INTERNALS----------------------------------------------

func (pgSync *PgSync) loadIndices() error {
	pgSync.indices = make(map[string]*types.Index)
	for _, mapping := range pgSync.config.Mappings {
		var index types.Index
		index.Init(mapping)

		//Set subscriber in index
		subscriberName, ok := mapping["in"]
		if !ok {
			subscriberName = pgSync.config.DefaultIn
		}
		var subscriber types.AbstractSubscriber
		subscriber, err := pgSync.GetSubscriber(subscriberName.(string))
		if err != nil {
			return err
		}
		index.SetSubscriber(&subscriber)

		//Set publishers to index
		publisherNames := pgSync.config.DefaultOut
		mappingOuts, ok := mapping["out"]
		if ok {
			publisherNames = []string{}
			for _, mappingOut := range mappingOuts.([]interface{}) {
				publisherNames = append(publisherNames, mappingOut.(string))
			}
		}
		for _, name := range publisherNames {
			publisher, err := pgSync.GetPublisher(name)
			if err != nil {
				return err
			}
			index.AddPublisher(&publisher)
		}
		pgSync.indices[index.Name] = &index
	}
	return nil
}
func (pgSync *PgSync) loadSubscribers() error {
	pgSync.subscribers = make(map[string]types.AbstractSubscriber)
	for name, config := range pgSync.config.In {
		var subscriber types.AbstractSubscriber
		switch config["driver"] {
		case "pgxpool-trigger":
			subscriber = &postgresql.Subscriber{}
		default:
			return fmt.Errorf("invalid In Driver: %s", config["driver"])
		}
		pgSync.subscribers[name] = subscriber
	}
	return nil
}
func (pgSync *PgSync) loadPublishers() error {
	pgSync.publishers = make(map[string]types.AbstractPublisher)

	for name, config := range pgSync.config.Out {
		var publisher types.AbstractPublisher
		switch config["driver"] {
		case "elastic":
			publisher = &elastic.Publisher{}
		default:
			return fmt.Errorf("invalid Out Driver: %s", config["driver"])
		}
		pgSync.publishers[name] = publisher
	}
	return nil
}

func (pgSync *PgSync) initSubscribers() error {
	for name, subscriber := range pgSync.GetSubscribers() {
		subscribersConfig := utils.CopyableMap(pgSync.config.In[name]).DeepCopy()
		delete(subscribersConfig, "driver")
		subscriber.InternalInit(&pgSync.eventChannel, name)
		subscriber.Init(subscribersConfig)
	}
	return nil
}
func (pgSync *PgSync) initPublishers() error {
	for name, publisher := range pgSync.GetPublishers() {
		publisherConfig := utils.CopyableMap(pgSync.config.Out[name]).DeepCopy()
		delete(publisherConfig, "driver")
		publisher.InternalInit(name)
		subs := pgSync.getIndicesForPublisher(publisher)
		publisher.Init(publisherConfig, subs)
	}
	return nil
}

func (pgSync *PgSync) getIndicesForSubscriber(subscriber types.AbstractSubscriber) []*types.Index {
	var indices []*types.Index
	for _, index := range pgSync.indices {
		if (*index.Subscriber) == subscriber {
			indices = append(indices, index)
		}
	}
	return indices
}
func (pgSync *PgSync) getIndicesForPublisher(publisher types.AbstractPublisher) []*types.Index {
	var indices []*types.Index
	for _, index := range pgSync.indices {
		for _, indexPublisher := range index.Publishers {
			if publisher == (*indexPublisher) {
				indices = append(indices, index)
			}
		}

	}
	return indices
}
