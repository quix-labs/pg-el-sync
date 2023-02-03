package elastic

import (
	"bytes"
	"encoding/json"
	elasticsearch8 "github.com/elastic/go-elasticsearch/v8"
	"go_pg_es_sync/internals/types"
	"go_pg_es_sync/publishers"
	"sync"
)

type Publisher struct {
	sync.RWMutex
	publishers.Publisher
	client *elasticsearch8.Client
}

func (p *Publisher) Init(config map[string]interface{}, _ []types.Index) {
	esConfig := elasticsearch8.Config{}
	endpoints, exists := config["endpoints"]
	if exists && endpoints != nil {
		for _, endpoint := range endpoints.([]interface{}) {
			esConfig.Addresses = append(esConfig.Addresses, endpoint.(string))
		}
	}
	username, exists := config["username"]
	if exists && username != nil {
		esConfig.Username = username.(string)
	}
	password, exists := config["password"]
	if exists && password != nil {
		esConfig.Password = password.(string)
	}
	es8, err := elasticsearch8.NewClient(esConfig)
	if err != nil {
		p.Logger.Fatal().Err(err).Msg("Unable to connect to elasticsearch")
	}
	_, err = es8.Info()
	if err != nil {
		p.Logger.Fatal().Err(err).Msg("Unable to ping elasticsearch")
	}
	p.Logger.Print("Successfully connected to elasticsearch")
	p.client = es8
}

func (p *Publisher) Insert(rows []*types.InsertsRow) {
	var body [][]byte
	for _, row := range rows {
		data, err := json.Marshal(row.Record)
		if err != nil {
			p.Logger.Print(err)
			continue
		}
		body = append(body, []byte(`{"index":{"_index":"`+row.Index+`","_id":"`+row.Reference+`"}}`), data)
	}
	p.sendBulk(body)
}

func (p *Publisher) Update(rows []*types.UpdateRow) {
	var body [][]byte
	for _, row := range rows {
		data, err := json.Marshal(row.Record)
		if err != nil {
			p.Logger.Print(err)
			continue
		}
		body = append(body, []byte(`{"index":{"_index":"`+row.Index+`","_id":"`+row.Reference+`"}}`), data)
	}
	p.sendBulk(body)
}

func (p *Publisher) Delete(rows []*types.DeleteRow) {
	var body [][]byte
	for _, row := range rows {
		body = append(body, []byte(`{"delete":{"_index":"`+row.Index+`","_id":"`+row.Reference+`"}}`))
	}
	p.sendBulk(body)
}

func (p *Publisher) sendBulk(rows [][]byte) {
	p.Lock()
	defer p.Unlock()
	fullBody := append(bytes.Join(rows, []byte("\n")), "\n"...)
	res, err := p.client.Bulk(bytes.NewReader(fullBody))
	if err != nil {
		p.Logger.Err(err)
		return
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		p.Logger.Printf("Error sending bulk request", res.String())
	}

}

func (p *Publisher) Terminate() {}
