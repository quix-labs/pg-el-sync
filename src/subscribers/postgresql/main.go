package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go_pg_es_sync/internals/types"
	"go_pg_es_sync/subscribers"
	"os"
	"strconv"
	"strings"
)

const eventName = "pgsync_event_golang"
const notifyTriggerFunctionName = "pgsync_notify_trigger_golang"

type Subscriber struct {
	subscribers.Subscriber
	conn *pgxpool.Pool
}

func (pg *Subscriber) Init(config map[string]any, indices []types.Index) {
	connString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config["host"], config["port"], config["username"], config["password"], config["database"],
	)
	pgxpool, err := pgxpool.New(context.Background(), connString+" application_name=PgSync_Listener")
	if err != nil {
		pg.Logger.Printf("Unable to connect to database: %v", err)
	}
	pg.Logger.Printf("Successfully connected to %s@%s/%s", config["username"], config["host"], config["database"])
	pg.conn = pgxpool
	pg.prepare(indices)
}

func (pg *Subscriber) prepare(indices []types.Index) {
	// Create trigger function
	_, err := pg.conn.Exec(context.Background(), fmt.Sprintf(`
CREATE OR REPLACE FUNCTION "%s"() RETURNS trigger AS $trigger$
DECLARE
  payload TEXT;
BEGIN
  IF TG_OP <> 'UPDATE' OR NEW IS DISTINCT FROM OLD THEN
    -- Build the payload
    payload := json_build_object(
        -- 'timestamp',CURRENT_TIMESTAMP,
        'action',LOWER(TG_OP),
        -- 'schema',TG_TABLE_SCHEMA,
        'table',TG_TABLE_NAME,
        'reference',COALESCE(NEW.id, OLD.id)
    );
    PERFORM pg_notify('%s',payload);
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$trigger$ LANGUAGE plpgsql VOLATILE;
`, notifyTriggerFunctionName, eventName))
	if err != nil {
		pg.Logger.Printf("Error create trigger function: %v\n", err)
		os.Exit(1)
	}

	var tables []string
	for _, index := range indices {
		tables = append(tables, index.Table)
		tables = append(tables, index.GetRelationsDependenciesTables()...)
	}
	//Create trigger on tables
	for _, table := range tables {
		createTriggerSql := fmt.Sprintf(`CREATE OR REPLACE TRIGGER pgync_%s_trigger AFTER DELETE OR UPDATE OR INSERT ON %s FOR EACH ROW EXECUTE PROCEDURE %s();`, table, table, notifyTriggerFunctionName)
		_, err = pg.conn.Exec(context.Background(), createTriggerSql)
		if err != nil {
			pg.Logger.Printf("Error create trigger for table %s: %v\n", table, err)
			os.Exit(1)
		}
	}
}

func (pg *Subscriber) Listen() {
	persistentConn, err := pg.conn.Acquire(context.Background())
	if err != nil {
		pg.Logger.Printf("Cannot get listen connection: %s", err)
	}
	listenConn := persistentConn.Conn()
	_, err = listenConn.Exec(context.Background(), "listen "+eventName)
	if err != nil {
		pg.Logger.Printf("Error listening to channel: %s", err)
		os.Exit(1)
	}

	for {
		notification, err := listenConn.WaitForNotification(context.Background())
		if err != nil {
			pg.Logger.Printf("Error waiting for notification: %s", err)
			os.Exit(1)
		}
		event, err := pg.parseNotification(notification)
		if err != nil {
			pg.Logger.Print(err)
			continue
		}
		pg.DispatchEvent(event)
	}
}

func (pg *Subscriber) parseNotification(notification *pgconn.Notification) (*interface{}, error) {
	var res struct {
		Action    string
		Table     string
		Reference int
	}
	err := json.Unmarshal([]byte(notification.Payload), &res)
	if err != nil {
		return nil, err
	}
	var event interface{}
	switch res.Action {
	case "insert":
		event = types.InsertEvent{
			Table:     res.Table,
			Reference: strconv.Itoa(res.Reference),
		}
	case "update":
		event = types.UpdateEvent{
			Table:     res.Table,
			Reference: strconv.Itoa(res.Reference),
		}
	case "delete":
		event = types.DeleteEvent{
			Table:     res.Table,
			Reference: strconv.Itoa(res.Reference),
		}
	default:
		return nil, fmt.Errorf("Unable to parse event with action: %s ", res.Action)
	}
	return &event, nil
}

func (pg *Subscriber) Terminate() {
	defer pg.conn.Close()
}

//-----------------------------------------READ INDEX/DOCUMENTS---------------------------------------------

func (pg *Subscriber) GetAllRecordsForIndex(index *types.Index) <-chan types.Record {
	chnl := make(chan types.Record)
	baseQuery := pg.getQueryForIndex(index)
	go func() {
		prevId := 0
		rowsCount := index.ChunkSize
		for rowsCount >= index.ChunkSize {
			rowsCount = 0
			query := fmt.Sprintf(
				`%s WHERE "%s"."id" > %d ORDER BY "id" ASC LIMIT %d`,
				baseQuery,
				index.Table,
				prevId,
				index.ChunkSize,
			)
			rows, err := pg.conn.Query(context.Background(), query)
			if err != nil {
				pg.Logger.Printf("Cannot execute query: %s", err)
			}
			for rows.Next() {
				var jsonRowResult []byte
				var reference int
				err := rows.Scan(&jsonRowResult, &reference)
				if err != nil {
					pg.Logger.Printf("Error fetching row: %s", err)
					continue
				}

				//Parse DB JSON result
				var fullRecord map[string]interface{}
				err = json.Unmarshal(jsonRowResult, &fullRecord)
				if err != nil {
					pg.Logger.Printf("Cannot parse json for row: %s", err)
					continue
				}

				rowsCount++
				prevId = reference
				chnl <- types.Record{Reference: strconv.Itoa(reference), Data: fullRecord}
			}
			rows.Close()
		}
		// Ensure that at the end of the loop we close the channel!
		close(chnl)
	}()
	return chnl
}

func (pg *Subscriber) GetFullRecordsForIndex(references []string, index *types.Index) (map[string]map[string]interface{}, error) {
	sqlQuery := fmt.Sprintf(
		`%s WHERE "%s"."id" IN (%s)`, pg.getQueryForIndex(index),
		index.Table,
		strings.Join(references, ","),
	)
	//pg.readMutex.Lock()
	rows, err := pg.conn.Query(context.Background(), sqlQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fullRecords := map[string]map[string]interface{}{}
	for rows.Next() {
		var jsonRowResult []byte
		var reference string
		err := rows.Scan(&jsonRowResult, &reference)
		if err != nil {
			pg.Logger.Printf("Error fetching row: %s\n", err)
			continue
		}

		//Parse DB JSON result
		var fullRecord map[string]interface{}
		err = json.Unmarshal(jsonRowResult, &fullRecord)
		if err != nil {
			pg.Logger.Printf("Cannot parse json for row: %s\n", err)
			continue
		}

		fullRecords[reference] = fullRecord
	}
	//pg.readMutex.Unlock()
	return fullRecords, nil

}

func (pg *Subscriber) getQueryForIndex(idx *types.Index) string {
	index := Index(*idx)
	query := index.GetSelectQuery()
	return query
}
func (pg *Subscriber) GetFullRecordsForRelationUpdate(relationUpdates types.RelationsUpdate, idx *types.Index) (map[string]map[string]interface{}, error) {
	index := Index(*idx)
	sqlQuery := index.GetSelectQuery() + " " + index.GetWhereRelationQuery(relationUpdates)
	fmt.Println(index.GetWhereRelationQuery(relationUpdates))
	rows, err := pg.conn.Query(context.Background(), sqlQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fullRecords := map[string]map[string]interface{}{}
	for rows.Next() {
		var jsonRowResult []byte
		var reference string
		err := rows.Scan(&jsonRowResult, &reference)
		if err != nil {
			pg.Logger.Printf("Error fetching row: %s\n", err)
			continue
		}

		//Parse DB JSON result
		var fullRecord map[string]interface{}
		err = json.Unmarshal(jsonRowResult, &fullRecord)
		if err != nil {
			pg.Logger.Printf("Cannot parse json for row: %s\n", err)
			continue
		}
		fullRecords[reference] = fullRecord
	}
	return fullRecords, nil
}

// ------------------------------------------HANDLE RELATIONSHIPS---------------------------------------------
//type RelationUpdate struct {
//	Relation   *types.CommonRelation
//	References []string
//}
//
//func (pg *Subscriber) getAllRecordsForRelation(relationUpdates []RelationUpdate, index *types.Index) {

/*
	where exists (
	    select * from "posts" where "users"."id" = "posts"."user_id"
	    and exists (
	        select * from "users" where "posts"."user_id" = "users"."id" and "id" = 1
	    )
	)
	or exists (
	    select * from "posts" where "users"."id" = "posts"."user_id"
	    and exists (
	        select * from "users" where "posts"."user_id" = "users"."id" and "id" = 2
	    )
	)
*/
//}
