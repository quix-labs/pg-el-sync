package elastic

import (
	"bytes"
	"encoding/json"
	"errors"
	elasticsearch8 "github.com/elastic/go-elasticsearch/v8"
	"go_pg_es_sync/internals/types"
	"go_pg_es_sync/publishers"
	"sync"
)

type Publisher struct {
	sync.RWMutex
	publishers.Publisher
	client *elasticsearch8.Client
	Prefix string
}

func (p *Publisher) Init(config map[string]interface{}, indices []*types.Index) {
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
	prefix, exists := config["prefix"]
	if exists && prefix != nil {
		p.Prefix = prefix.(string)
	}
	//esConfig.Logger = &elastictransport.JSONLogger{Output: os.Stdout}
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
	err = p.prepareIndices(indices)
	if err != nil {
		p.Logger.Fatal().Err(err).Msg("Cannot create index")
	}

}

func (p *Publisher) Insert(rows []*types.InsertsRow) {
	var body [][]byte
	for _, row := range rows {
		data, err := json.Marshal(row.Record)
		if err != nil {
			p.Logger.Print(err)
			continue
		}
		body = append(body, []byte(`{"index":{"_index":"`+p.Prefix+row.Index+`","_id":"`+row.Reference+`"}}`), data)
	}
	//p.Logger.Debug().Msgf("SEND INSERT BULK - SIZE: %d", len(body))
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
		body = append(body, []byte(`{"index":{"_index":"`+p.Prefix+row.Index+`","_id":"`+row.Reference+`"}}`), data)
	}
	//p.Logger.Debug().Msgf("SEND UPDATE BULK - SIZE: %d", len(body))
	p.sendBulk(body)
}

func (p *Publisher) Delete(rows []*types.DeleteRow) {
	var body [][]byte
	for _, row := range rows {
		body = append(body, []byte(`{"delete":{"_index":"`+p.Prefix+row.Index+`","_id":"`+row.Reference+`"}}`))
	}
	//p.Logger.Debug().Msgf("SEND DELETE BULK - SIZE: %d", len(body))
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

func (p *Publisher) prepareIndices(indices []*types.Index) error {

	for _, index := range indices {
		if index.Settings == nil {
			continue
		}

		res, err := p.client.Indices.Exists([]string{index.Name}, p.client.Indices.Exists.WithPretty())
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.IsError() {
			res, err := p.client.Indices.Create(index.Name, p.client.Indices.Create.WithHuman())
			if err != nil {
				return err
			}
			if res.IsError() {
				return errors.New(res.String())
			}
		}

		if mappings, exists := index.Settings["mappings"]; exists && mappings != nil {
			body, err := json.Marshal(mappings)
			if err != nil {
				return err
			}
			res, err = p.client.Indices.PutMapping([]string{index.Name}, bytes.NewReader(body))
			if err != nil {
				return err
			}
			defer res.Body.Close()
			if res.IsError() {
				return errors.New(res.String())
			}
		}

	}
	return nil

}
