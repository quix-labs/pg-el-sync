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

	ReferenceField string

	Subscriber *AbstractSubscriber
	Publishers []*AbstractPublisher
	ChunkSize  int

	WaitingEvents *WaitingEvents

	Logger *zerolog.Logger
	Active bool
}

type RelationsUpdate map[*Relation][]string

func (index *Index) Init(config map[string]interface{}) {
	log := zerolog.New(os.Stdout).With().Caller().Stack().Timestamp().Str("service", "index").Logger()
	index.Logger = &log
	index.WaitingEvents = &WaitingEvents{}
	err := index.Parse(config)
	if err != nil {
		return
	}

	index.GetRelationsDependenciesTables()
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
			indexedResults := map[string]*InsertEvent{}
			var references []string

			for _, event := range results {
				indexedResults[event.Reference] = event
				references = append(references, event.Reference)
			}

			fullRecords, err := (*index.Subscriber).GetFullRecordsForIndex(references, index)
			if err != nil {
				fmt.Println("Error getting full record", err)
				continue
			}

			var rows []*InsertsRow
			for reference, record := range fullRecords {
				rows = append(rows, &InsertsRow{
					Reference: indexedResults[reference].Reference,
					Index:     index.Name,
					Record:    record,
				})
			}

			for _, publisher := range index.Publishers {
				(*publisher).Insert(rows)
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
			indexedResults := map[string]*UpdateEvent{}
			var references []string

			for _, event := range results {
				indexedResults[event.Reference] = event
				references = append(references, event.Reference)
			}

			fullRecords, err := (*index.Subscriber).GetFullRecordsForIndex(references, index)
			if err != nil {
				fmt.Println("Error getting full record", err)
				continue
			}

			var rows []*UpdateRow
			for reference, record := range fullRecords {
				rows = append(rows, &UpdateRow{
					Reference: indexedResults[reference].Reference,
					Index:     index.Name,
					Record:    record,
				})
			}

			for _, publisher := range index.Publishers {
				(*publisher).Update(rows)
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
		for index.WaitingEvents.RelationsUpdate.Len() >= index.ChunkSize || millisecondUntilLastFetch > 500 {
			lastFetch = time.Now()
			millisecondUntilLastFetch = 0
			if index.WaitingEvents.RelationsUpdate.Len() == 0 {
				continue
			}
			results := index.WaitingEvents.RelationsUpdate.Retrieve(index.ChunkSize)

			indexedResults := RelationsUpdate{}

			for _, event := range results {
				for _, relation := range index.GetDependRelations(event.Table) {
					indexedResults[relation] = utils.Unique(append(indexedResults[relation], event.Reference))
				}
			}

			fullRecords, err := (*index.Subscriber).GetFullRecordsForRelationUpdate(indexedResults, index)
			if err != nil {
				fmt.Println("Error getting full record for update relation", err)
				continue
			}
			var rows utils.ConcurrentSlice[*UpdateRow]
			for reference, record := range fullRecords {
				rows.Append(&UpdateRow{
					Reference: reference,
					Index:     index.Name,
					Record:    record,
				})
			}
			fmt.Println(len(fullRecords))

			for rows.Len() > 0 {
				chunk := rows.Retrieve(index.ChunkSize)
				for _, publisher := range index.Publishers {
					(*publisher).Update(chunk)
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
			rows := insertRows.All()
			for _, publisher := range index.Publishers {
				(*publisher).Insert(rows)
			}
			insertRows.Clear()
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

	if _, exists := config["relations"]; exists {
		err = index.Relations.Parse(config["relations"], nil)
		if err != nil {
			index.Logger.Fatal().Err(err).Msg("Invalid table for mapping")
		}
	}
	return nil
}

func (index *Index) DependsOnTable(table string) bool {
	for _, dependTable := range index.GetRelationsDependenciesTables() {
		if dependTable == table {
			return true
		}
	}
	return false
}
func (index *Index) GetDependRelations(table string) []*Relation {
	var dependsRelations []*Relation
	for _, rel := range index.Relations {
		for _, depend := range rel.GetDependsRelations(table) {
			dependsRelations = append(dependsRelations, depend)
		}
	}
	return dependsRelations
}
func (index *Index) GetRelationsDependenciesTables() []string {
	var dependenciesTables []string
	for _, rel := range index.Relations {
		for _, table := range rel.GetDependenciesTables() {
			if table == index.Table {
				continue
			}
			dependenciesTables = append(dependenciesTables, table)
		}
	}
	dependenciesTables = utils.Unique(dependenciesTables)

	return dependenciesTables
}
