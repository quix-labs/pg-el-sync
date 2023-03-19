package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go_pg_es_sync/internals/types"
	"go_pg_es_sync/internals/utils"
	"go_pg_es_sync/subscribers"
	"os"
	"strconv"
	"strings"
)

const (
	ApplicationName             = "PgSync_Listener"
	PoolMinConn                 = 2
	PoolMaxConn                 = 5
	EventName                   = "pgsync_event"
	NotifyTriggerFunctionPrefix = "pgsync_trigger"
	MaxRelationsFilter          = 50
	SchemaName                  = "pgsync"
)

type Subscriber struct {
	subscribers.Subscriber
	conn *pgxpool.Pool
}

func (pg *Subscriber) Init(config map[string]any) {
	connConf, err := pgxpool.ParseConfig("")
	if err != nil {
		pg.Logger.Fatal().Err(err)
	}
	connConf.ConnConfig.Config.RuntimeParams["application_name"] = ApplicationName
	connConf.MinConns = PoolMinConn
	connConf.MaxConns = PoolMaxConn
	_ = utils.ParseMapKey(config, "host", &connConf.ConnConfig.Config.Host)
	_ = utils.ParseMapKey(config, "port", &connConf.ConnConfig.Config.Port)
	_ = utils.ParseMapKey(config, "database", &connConf.ConnConfig.Config.Database)
	_ = utils.ParseMapKey(config, "username", &connConf.ConnConfig.Config.User)
	_ = utils.ParseMapKey(config, "password", &connConf.ConnConfig.Config.Password)

	if pg.conn, err = pgxpool.NewWithConfig(context.TODO(), connConf); err != nil {
		pg.Logger.Fatal().Err(err).Msgf("Unable to connect to database: %v", err)
	}
	_, err = pg.conn.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE; CREATE SCHEMA "%s"`, SchemaName, SchemaName))
	if err != nil {
		pg.Logger.Fatal().Err(err).Msg("Error create schema")
	}
	pg.Logger.Printf("Successfully connected to %s@%s/%s", config["username"], config["host"], config["database"])
}

func (pg *Subscriber) Listen() {
	persistentConn, err := pg.conn.Acquire(context.Background())
	if err != nil {
		pg.Logger.Printf("Cannot get listen connection: %s", err)
	}
	listenConn := persistentConn.Conn()
	_, err = listenConn.Exec(context.Background(), "listen "+EventName)
	if err != nil {
		pg.Logger.Fatal().Err(err).Msgf("Error listening to channel: %s", err)
	}

	for {
		notification, err := listenConn.WaitForNotification(context.Background())
		if err != nil {
			pg.Logger.Fatal().Err(err).Msgf("Error waiting for notification: %s", err)
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
		Type           string `json:"type"`
		Index          string `json:"index"`
		Relation       string `json:"relation"`
		Action         string `json:"action"`
		Reference      string `json:"reference"`
		SoftDeleted    bool   `json:"soft_deleted"`
		OldSoftDeleted bool   `json:"old_soft_deleted"`
		Local          string `json:"local"`
		OldLocal       string `json:"old_local"`
		Related        string `json:"related"`
		OldRelated     string `json:"old_related"`
	}
	err := json.Unmarshal([]byte(notification.Payload), &res)
	if err != nil {
		return nil, err
	}
	var event interface{}

	if res.Type == "table" {
		switch res.Action {
		case "insert":
			event = types.InsertEvent{
				Index:     res.Index,
				Reference: res.Reference,
			}
		case "update":
			event = types.UpdateEvent{
				Index:                 res.Index,
				Reference:             res.Reference,
				SoftDeleted:           res.SoftDeleted,
				PreviouslySoftDeleted: res.OldSoftDeleted,
			}
		case "delete":
			event = types.DeleteEvent{
				Index:     res.Index,
				Reference: res.Reference,
			}
		default:
			return nil, fmt.Errorf("unable to parse event with action: %s ", res.Action)
		}
		return &event, nil
	}

	if res.Type == "relation" {
		event = types.RelationUpdateEvent{
			Index:     res.Index,
			Relation:  res.Relation,
			Reference: res.Reference,
		}
		return &event, nil
	}
	if res.Type == "relation_pivot" {
		event = types.RelationUpdateEvent{
			Index:     res.Index,
			Relation:  res.Relation,
			Reference: res.Local,
			Pivot:     true,
		}
		if res.OldRelated != "" && res.OldRelated != res.Related {
			event = types.RelationUpdateEvent{
				Index:     res.Index,
				Relation:  res.Relation,
				Reference: res.OldLocal,
				Pivot:     true,
			}
		}
		return &event, nil
	}
	return nil, fmt.Errorf("unable to parse event")

}

func (pg *Subscriber) Terminate() {
	defer pg.conn.Close()
}

// -----------------------------------------------PREPARATION------------------------------------------------

func (pg *Subscriber) PrepareListen(indices []*types.Index) {
	for _, index := range indices {
		pg.initIndexListener(index)
		for _, relation := range index.GetAllRelations() {
			pg.initRelationListener(relation, index)
		}
	}
}

func (pg *Subscriber) initIndexListener(index *types.Index) {
	whereOkSql, oldWhereOkSql := "true", "true"
	if len(index.Wheres) > 0 {
		wheres := Wheres(index.Wheres)
		whereOkSql = "(" + wheres.GetConditionSql("NEW", true) + ")"
		oldWhereOkSql = "(" + wheres.GetConditionSql("OLD", true) + ")"
	}
	functionName := NotifyTriggerFunctionPrefix + "_" + index.Table
	_, err := pg.conn.Exec(context.Background(), fmt.Sprintf(`
CREATE OR REPLACE FUNCTION "%s"."%s"() RETURNS trigger AS $trigger$
BEGIN
  IF TG_OP <> 'UPDATE' OR NEW IS DISTINCT FROM OLD THEN
    PERFORM pg_notify('%s', json_build_object(
		'type', 'table',
		'index', '%s',
        'action', LOWER(TG_OP),
        'reference',COALESCE(NEW."%s", OLD."%s")::TEXT,
        'soft_deleted',NOT (%s),
        'old_soft_deleted',NOT (%s)
    )::TEXT);
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$trigger$ LANGUAGE plpgsql VOLATILE;
`, SchemaName, functionName, EventName, index.Name, index.ReferenceField, index.ReferenceField, whereOkSql, oldWhereOkSql))
	if err != nil {
		pg.Logger.Printf("Error create trigger function: %v\n", err)
		os.Exit(1)
	}
	triggerName := "pgsync" + index.Table + "_trigger"
	sql := fmt.Sprintf(
		`CREATE OR REPLACE TRIGGER %s AFTER DELETE OR UPDATE OR INSERT ON %s FOR EACH ROW EXECUTE PROCEDURE "%s"."%s"();`,
		triggerName,
		index.Table,
		SchemaName,
		functionName,
	)
	_, err = pg.conn.Exec(context.Background(), sql)
	if err != nil {
		pg.Logger.Fatal().Msgf("Error create trigger: %v", err)
	}
}
func (pg *Subscriber) initRelationListener(relation *types.Relation, index *types.Index) {
	functionName := NotifyTriggerFunctionPrefix + "_" + index.Name + "_rel_" + relation.UniqueName
	_, err := pg.conn.Exec(context.Background(), fmt.Sprintf(`
CREATE OR REPLACE FUNCTION "%s"."%s"() RETURNS trigger AS $trigger$
BEGIN
  IF (TG_OP <> 'UPDATE' OR NEW IS DISTINCT FROM OLD) AND COALESCE(NEW."%s",OLD."%s") IS NOT NULL THEN
    PERFORM pg_notify('%s', json_build_object(
		'type', 'relation',
        'index', '%s',
        'relation','%s',
        'reference',COALESCE(NEW."%s", OLD."%s")::TEXT
    )::TEXT);
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$trigger$ LANGUAGE plpgsql VOLATILE;
`, SchemaName, functionName, relation.ForeignKey.Local, relation.ForeignKey.Local, EventName, index.Name, relation.UniqueName, relation.ForeignKey.Local, relation.ForeignKey.Local))
	if err != nil {
		pg.Logger.Fatal().Msgf("Error create trigger function: %v", err)
	}
	triggerName := "pgsync_rel_" + index.Name + "_" + relation.UniqueName
	sql := fmt.Sprintf(
		`CREATE OR REPLACE TRIGGER %s AFTER DELETE OR UPDATE OR INSERT ON %s FOR EACH ROW EXECUTE PROCEDURE "%s"."%s"();`,
		triggerName,
		relation.Table,
		SchemaName,
		functionName,
	)
	_, err = pg.conn.Exec(context.Background(), sql)
	if err != nil {
		pg.Logger.Fatal().Msgf("Error create trigger: %v", err)
	}
	if relation.Type == "many_to_many" {
		pg.initPivotRelationListener(relation, index)
	}
}

func (pg *Subscriber) initPivotRelationListener(relation *types.Relation, index *types.Index) {
	functionName := NotifyTriggerFunctionPrefix + "_" + index.Name + "_rel_pivot_" + relation.UniqueName
	_, err := pg.conn.Exec(context.Background(), fmt.Sprintf(`
CREATE OR REPLACE FUNCTION "%s"."%s"() RETURNS trigger AS $trigger$
BEGIN
  IF TG_OP <> 'UPDATE' OR NEW IS DISTINCT FROM OLD THEN
    PERFORM pg_notify('%s', json_build_object(
		'type', 'relation_pivot',
        'index', '%s',
        'relation','%s',
        'local',COALESCE(NEW."%s", null)::TEXT,
        'old_local',COALESCE(OLD."%s", null)::TEXT,
        'related',COALESCE(NEW."%s", null)::TEXT,
        'old_related',COALESCE(OLD."%s", null)::TEXT
    )::TEXT);
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$trigger$ LANGUAGE plpgsql VOLATILE;
`, SchemaName, functionName, EventName, index.Name, relation.UniqueName, relation.ForeignKey.PivotLocal, relation.ForeignKey.PivotLocal, relation.ForeignKey.PivotRelated, relation.ForeignKey.PivotRelated))
	if err != nil {
		pg.Logger.Fatal().Msgf("Error create trigger function: %v", err)
	}
	triggerName := "pgsync_rel_pivot_" + index.Name + "_" + relation.UniqueName
	sql := fmt.Sprintf(
		`CREATE OR REPLACE TRIGGER %s AFTER DELETE OR UPDATE OR INSERT ON %s FOR EACH ROW EXECUTE PROCEDURE "%s"."%s"();`,
		triggerName,
		relation.ForeignKey.PivotTable,
		SchemaName,
		functionName,
	)
	_, err = pg.conn.Exec(context.Background(), sql)
	if err != nil {
		pg.Logger.Fatal().Msgf("Error create trigger: %v", err)
	}
}

//-----------------------------------------READ INDEX/DOCUMENTS---------------------------------------------

func (pg *Subscriber) GetAllRecordsForIndex(index *types.Index) <-chan types.Record {
	wheresSqlRaw := pg.GetWhereQuery(index)
	query := pg.getSelectQuery(index) + " " + wheresSqlRaw
	//@TODO Clean code
	materializedViewName := "pgsync_temp_view_" + index.Name
	_, err := pg.conn.Exec(context.Background(), fmt.Sprintf(`DROP MATERIALIZED VIEW IF EXISTS "%s"."%s"`, SchemaName, materializedViewName))
	if err != nil {
		pg.Logger.Fatal().Msgf("Error create trigger: %v", err)
	}
	_, err = pg.conn.Exec(context.Background(), fmt.Sprintf(`CREATE MATERIALIZED VIEW "%s"."%s" AS(%s)`, SchemaName, materializedViewName, query))
	if err != nil {
		pg.Logger.Fatal().Msgf("Error create trigger: %v", err)
	}

	query = "SELECT * FROM " + SchemaName + "." + materializedViewName
	index.ReferenceField = "reference"
	index.Table = SchemaName + `"."` + materializedViewName //@TODO Clean code
	return pg.getQueryRecords(query, index, false)

	return pg.getQueryRecords(query, index, wheresSqlRaw != "")
}
func (pg *Subscriber) GetFullRecordsForIndex(references []string, index *types.Index) <-chan types.Record {
	wheresSqlRaw := pg.GetWhereQuery(index)
	query := pg.getSelectQuery(index) + " " + wheresSqlRaw
	if wheresSqlRaw != "" {
		query += fmt.Sprintf(
			` AND "%s"."%s" IN (%s)`, index.Table, index.ReferenceField, strings.Join(references, ","),
		)
	} else {
		query += fmt.Sprintf(
			`WHERE "%s"."%s" IN (%s)`, index.Table, index.ReferenceField, strings.Join(references, ","),
		)
	}

	return pg.getQueryRecords(query, index, true)
}

func (pg *Subscriber) GetFullRecordsForRelationUpdate(relationUpdates types.RelationsUpdate, idx *types.Index) <-chan types.Record {
	ch := make(chan types.Record)

	go func() {
		index := Index(*idx)
		var getRecords = func(relationUpdates types.RelationsUpdate) {
			wheresSqlRaw := pg.GetWhereQuery(idx)
			wheresRelationRaw := index.GetWhereRelationQuery(relationUpdates)

			sqlQuery := ""
			if wheresSqlRaw == "" {
				sqlQuery = index.GetSelectQuery() + " " + wheresRelationRaw
			} else {
				wheresRelationRaw = strings.TrimPrefix(wheresRelationRaw, "WHERE ")
				wheresRelationRaw = "AND " + wheresRelationRaw
				sqlQuery = index.GetSelectQuery() + " " + wheresSqlRaw + " " + wheresRelationRaw
			}
			for row := range pg.getQueryRecords(sqlQuery, idx, true) {
				ch <- row
			}
		}

		/**Group to filter max 50 relations*/
		chunkRelationUpdates := make(types.RelationsUpdate)
		currentRelationSize := 0
		for relation, references := range relationUpdates {
			for _, reference := range references {
				currentRelationSize++
				chunkRelationUpdates[relation] = append(chunkRelationUpdates[relation], reference)

				if currentRelationSize >= MaxRelationsFilter {
					getRecords(chunkRelationUpdates)
					chunkRelationUpdates = make(types.RelationsUpdate)
					currentRelationSize = 0
				}

			}
		}
		if currentRelationSize > 0 {
			getRecords(chunkRelationUpdates)
		}
		close(ch)
	}()
	return ch
}

// ---------------------------------------------INTERNALS----------------------------------------------------------------

func (pg *Subscriber) GetConditionQuery(index *types.Index) string {
	wheres := Wheres(index.Wheres)
	wheresSqlRaw := wheres.GetConditionSql(index.Table, false)
	return wheresSqlRaw
}
func (pg *Subscriber) GetWhereQuery(index *types.Index) string {
	wheres := Wheres(index.Wheres)
	wheresSqlRaw := wheres.GetWhereSql(index.Table)
	return wheresSqlRaw
}
func (pg *Subscriber) getSelectQuery(idx *types.Index) string {
	index := Index(*idx)
	query := index.GetSelectQuery()
	return query
}

func (pg *Subscriber) getQueryRecords(query string, index *types.Index, useAnd bool) <-chan types.Record {
	ch := make(chan types.Record)
	baseQuery := query
	go func() {
		defer close(ch)

		prevId := 0
		rowsCount := index.ChunkSize
		for rowsCount >= index.ChunkSize {
			rowsCount = 0
			operator := "WHERE"
			if useAnd {
				operator = "AND"
			}
			query := fmt.Sprintf(
				`%s %s "%s"."%s" > %d ORDER BY "%s" ASC LIMIT %d`,
				baseQuery,
				operator,
				index.Table,
				index.ReferenceField,
				prevId,
				index.ReferenceField,
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
				ch <- types.Record{Reference: strconv.Itoa(reference), Data: fullRecord}
			}
			rows.Close()
		}
	}()

	return ch
}
