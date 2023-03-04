package types

import (
	"fmt"
	"github.com/rs/zerolog"
	"go_pg_es_sync/internals/utils"
	"os"
	"time"
)

type Index struct {
	Name      string
	Table     string
	Fields    Fields
	Relations Relations
	Wheres    Wheres

	ReferenceField string
	Settings       map[string]any

	Subscriber *AbstractSubscriber
	Publishers []*AbstractPublisher
	ChunkSize  int

	WaitingEvents *WaitingEvents

	Logger *zerolog.Logger
	Active bool
}

type RelationsUpdate map[*Relation][]*RelationUpdateEvent

func (index *Index) Init(config map[string]interface{}) {
	log := zerolog.New(os.Stdout).With().Caller().Stack().Timestamp().Str("service", "index").Logger()
	index.Logger = &log
	index.WaitingEvents = &WaitingEvents{}
	err := index.Parse(config)
	if err != nil {
		return
	}

	index.Active = true

	go index.asyncHandleInserts()
	go index.asyncHandleUpdates()
	go index.asyncHandleDeletes()
	go index.asyncHandleRelationsUpdates()
}

func (index *Index) SetSubscriber(subscriber *AbstractSubscriber) {
	index.Subscriber = subscriber
}
func (index *Index) AddPublisher(publisher *AbstractPublisher) {
	index.Publishers = append(index.Publishers, publisher)
}

//---------------------ASYNC EVENT HANDLERS---------------------------------

func (index *Index) asyncHandleInserts() {
	lastFetch := time.Now()
	for range time.Tick(time.Millisecond * 100) {
		millisecondUntilLastFetch := time.Now().Sub(lastFetch).Milliseconds()
		for index.WaitingEvents.Insert.Len() >= index.ChunkSize || millisecondUntilLastFetch > 500 {
			lastFetch = time.Now()
			millisecondUntilLastFetch = 0
			if index.WaitingEvents.Insert.Len() == 0 {
				continue
			}

			results := index.WaitingEvents.Insert.Retrieve(index.ChunkSize)

			var references []string
			for _, event := range results {
				references = append(references, event.Reference)
			}
			insertRows := utils.ConcurrentSlice[*InsertsRow]{}
			for row := range (*index.Subscriber).GetFullRecordsForIndex(references, index) {
				insertRows.Append(&InsertsRow{Index: index.Name, Record: row.Data, Reference: row.Reference})
				if insertRows.Len() >= index.ChunkSize {
					rows := insertRows.Retrieve(index.ChunkSize)
					for _, publisher := range index.Publishers {
						(*publisher).Insert(rows)
					}
				}
			}
			if insertRows.Len() > 0 {
				rows := insertRows.All()
				for _, publisher := range index.Publishers {
					(*publisher).Insert(rows)
				}
			}
		}
	}
}
func (index *Index) asyncHandleUpdates() {
	lastFetch := time.Now()
	for range time.Tick(time.Millisecond * 100) {
		millisecondUntilLastFetch := time.Now().Sub(lastFetch).Milliseconds()
		for index.WaitingEvents.Update.Len() >= index.ChunkSize || millisecondUntilLastFetch > 500 {
			lastFetch = time.Now()
			millisecondUntilLastFetch = 0
			if index.WaitingEvents.Update.Len() == 0 {
				continue
			}
			results := index.WaitingEvents.Update.Retrieve(index.ChunkSize)
			var references []string

			for _, event := range results {
				references = append(references, event.Reference)
			}
			updateRows := utils.ConcurrentSlice[*UpdateRow]{}
			for row := range (*index.Subscriber).GetFullRecordsForIndex(references, index) {
				updateRows.Append(&UpdateRow{Index: index.Name, Record: row.Data, Reference: row.Reference})
				if updateRows.Len() >= index.ChunkSize {
					rows := updateRows.Retrieve(index.ChunkSize)
					for _, publisher := range index.Publishers {
						(*publisher).Update(rows)
					}
				}
			}
			if updateRows.Len() > 0 {
				rows := updateRows.All()
				for _, publisher := range index.Publishers {
					(*publisher).Update(rows)
				}
			}
		}
	}
}
func (index *Index) asyncHandleDeletes() {
	//Async function to fetch events every 5sec
	lastFetch := time.Now()
	for range time.Tick(time.Millisecond * 100) {
		millisecondUntilLastFetch := time.Now().Sub(lastFetch).Milliseconds()
		for index.WaitingEvents.Delete.Len() >= index.ChunkSize || millisecondUntilLastFetch > 500 {
			lastFetch = time.Now()
			millisecondUntilLastFetch = 0
			if index.WaitingEvents.Delete.Len() == 0 {
				continue
			}
			results := index.WaitingEvents.Delete.Retrieve(index.ChunkSize)
			var rows []*DeleteRow
			for _, event := range results {
				rows = append(rows, &DeleteRow{
					Reference: event.Reference,
					Index:     index.Name,
				})
			}
			for _, publisher := range index.Publishers {
				(*publisher).Delete(rows)
			}
		}
	}
}
func (index *Index) asyncHandleRelationsUpdates() {
	lastFetch := time.Now()
	for range time.Tick(time.Millisecond * 100) {
		millisecondUntilLastFetch := time.Now().Sub(lastFetch).Milliseconds()
		for index.WaitingEvents.RelationsUpdate.Len() >= index.ChunkSize || millisecondUntilLastFetch > 1000 {
			lastFetch = time.Now()
			millisecondUntilLastFetch = 0
			if index.WaitingEvents.RelationsUpdate.Len() == 0 {
				continue
			}
			results := index.WaitingEvents.RelationsUpdate.Retrieve(index.ChunkSize)
			indexedResults := RelationsUpdate{}
			for _, event := range results {
				relation := index.GetAllRelations()[event.Relation]
				indexedResults[relation] = utils.Unique(append(indexedResults[relation], event))
			}

			updateRows := utils.ConcurrentSlice[*UpdateRow]{}
			for row := range (*index.Subscriber).GetFullRecordsForRelationUpdate(indexedResults, index) {
				updateRows.Append(&UpdateRow{Index: index.Name, Record: row.Data, Reference: row.Reference})

				if updateRows.Len() >= index.ChunkSize {
					rows := updateRows.Retrieve(index.ChunkSize)
					for _, publisher := range index.Publishers {
						(*publisher).Update(rows)
					}
				}
			}
			if updateRows.Len() > 0 {
				rows := updateRows.All()
				for _, publisher := range index.Publishers {
					(*publisher).Update(rows)
				}
			}
		}
	}
}

//------------------PREPARATION FUNCTIONS---------------------------------------

func (index *Index) IndexAllDocuments() {
	fmt.Printf("Index all documents for %s\n", index.Name)
	insertRows := utils.ConcurrentSlice[*InsertsRow]{}
	for row := range (*index.Subscriber).GetAllRecordsForIndex(index) {
		insertRows.Append(&InsertsRow{Index: index.Name, Record: row.Data, Reference: row.Reference})
		if insertRows.Len() >= index.ChunkSize {
			rows := insertRows.Retrieve(index.ChunkSize)
			for _, publisher := range index.Publishers {
				(*publisher).Insert(rows)
			}
		}
	}
	if insertRows.Len() > 0 {
		rows := insertRows.All()
		for _, publisher := range index.Publishers {
			(*publisher).Insert(rows)
		}
	}
}

// ----------------------------------PARSING--------------------------------------

func (index *Index) Parse(config map[string]interface{}) error {

	err := utils.ParseMapKey(config, "name", &index.Name)
	if err != nil {
		index.Logger.Fatal().Err(err).Msg("Invalid name for mapping")
	}
	err = utils.ParseMapKey(config, "settings", &index.Settings)
	if err != nil {
		index.Logger.Info().Msg("Invalid settings for mapping")
	}
	err = utils.ParseMapKey(config, "table", &index.Table)
	if err != nil {
		index.Logger.Fatal().Err(err).Msg("Invalid table for mapping")
	}
	err = utils.ParseMapKey(config, "reference_field", &index.ReferenceField)
	if err != nil {
		index.ReferenceField = "id"
		index.Logger.Info().Msg("Invalid or unspecified reference_field for mapping, default to id")
	}
	err = utils.ParseMapKey(config, "chunk_size", &index.ChunkSize)
	if err != nil {
		index.ChunkSize = 500
		index.Logger.Info().Msg("Invalid or unspecified chunk_size for mapping, default to 500")
	}

	if _, exists := config["fields"]; exists {
		err = index.Fields.Parse(config["fields"])
		if err != nil {
			index.Logger.Fatal().Err(err).Msg("Invalid table for mapping")
		}
	}
	if _, exists := config["wheres"]; exists {
		err = index.Wheres.Parse(config["wheres"])
		if err != nil {
			index.Logger.Fatal().Err(err).Msg("Invalid wheres for mapping")
		}
	}
	if _, exists := config["relations"]; exists {
		err = index.Relations.Parse(config["relations"], nil)
		if err != nil {
			index.Logger.Fatal().Err(err).Msg("Invalid table for mapping")
		}
	}

	return nil
}

func (index *Index) GetAllRelations() Relations {
	relations := make(Relations)
	for relName, rel := range index.Relations {
		relations[relName] = rel
		for subRelName, subRel := range rel.GetAllRelations() {
			relations[subRelName] = subRel
		}
	}
	return relations
}
