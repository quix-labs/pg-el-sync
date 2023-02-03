package internals

import (
	"fmt"
	"go_pg_es_sync/internals/types"
	"go_pg_es_sync/internals/utils"
	"go_pg_es_sync/publishers/elastic"
	"go_pg_es_sync/subscribers/postgresql"
)

type PgSync struct {
	config       *Config
	subscribers  map[string]types.AbstractSubscriber
	publishers   map[string]types.AbstractPublisher
	indices      []types.Index
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
		go subscriber.Listen()
	}

	for true {
		notification := <-pgSync.eventChannel
		switch event := (*notification).(type) {
		case types.DeleteEvent:
			for _, index := range pgSync.getIndicesForTable(event.Table) {
				index.WaitingEvents.Delete.Append(&event)
			}
			for _, index := range pgSync.getIndicesDependsOnTable(event.Table) {
				index.WaitingEvents.RelationsUpdate.Append(&types.RelationUpdateEvent{
					Table:     event.Table,
					Reference: event.Reference,
				})
			}
		case types.InsertEvent:
			for _, index := range pgSync.getIndicesForTable(event.Table) {
				index.WaitingEvents.Insert.Append(&event)
			}
			for _, index := range pgSync.getIndicesDependsOnTable(event.Table) {
				index.WaitingEvents.RelationsUpdate.Append(&types.RelationUpdateEvent{
					Table:     event.Table,
					Reference: event.Reference,
				})
			}
		case types.UpdateEvent:
			for _, index := range pgSync.getIndicesForTable(event.Table) {
				index.WaitingEvents.Update.Append(&event)
			}
			for _, index := range pgSync.getIndicesDependsOnTable(event.Table) {
				index.WaitingEvents.RelationsUpdate.Append(&types.RelationUpdateEvent{
					Table:     event.Table,
					Reference: event.Reference,
				})
			}
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
	for _, index := range pgSync.indices {
		index.IndexAllDocuments()
	}
}

// -----------------INTERNALS----------------------------------------------

func (pgSync *PgSync) loadIndices() error {
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
		pgSync.indices = append(pgSync.indices, index)
	}
	return nil
}
func (pgSync *PgSync) loadSubscribers() error {
	pgSync.subscribers = make(map[string]types.AbstractSubscriber)
	for name, config := range pgSync.config.In {
		var subscriber types.AbstractSubscriber
		switch config["driver"] {
		case "pg-trigger":
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
		subs := pgSync.getIndicesForSubscriber(subscriber)
		subscriber.Init(subscribersConfig, subs)
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
func (pgSync *PgSync) getIndicesForTable(table string) []types.Index {
	var indices []types.Index
	for _, index := range pgSync.indices {
		if index.Table == table {
			indices = append(indices, index)
		}
	}
	return indices
}
func (pgSync *PgSync) getIndicesDependsOnTable(table string) []types.Index {
	var indices []types.Index
	for _, index := range pgSync.indices {
		if index.DependsOnTable(table) {
			indices = append(indices, index)
		}
	}
	return indices
}
func (pgSync *PgSync) getIndicesForSubscriber(subscriber types.AbstractSubscriber) []types.Index {
	var indices []types.Index
	for _, index := range pgSync.indices {
		if (*index.Subscriber) == subscriber {
			indices = append(indices, index)
		}
	}
	return indices
}
func (pgSync *PgSync) getIndicesForPublisher(publisher types.AbstractPublisher) []types.Index {
	var indices []types.Index
	for _, index := range pgSync.indices {
		for _, indexPublisher := range index.Publishers {
			if publisher == (*indexPublisher) {
				indices = append(indices, index)
			}
		}

	}
	return indices
}
