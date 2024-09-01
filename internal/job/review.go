package job

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/segmentio/kafka-go"
	"review-job/internal/conf"
)

// 流式服务，
type JobWoker struct {
	kafkareader   *kafka.Reader
	elasticClient *ElasticSearch
	logger        *log.Helper
}
type ElasticSearch struct {
	elasticClient *elasticsearch.TypedClient
	index         string
}

type Message struct {
	Database string                   `json:"database"`
	IsDDL    bool                     `json:"isDDL"`
	Table    string                   `json:"table"`
	Type     string                   `json:"type"`
	Data     []map[string]interface{} `json:"data"`
}

func NewJobWorker(kafka *kafka.Reader, elastic *ElasticSearch, logger log.Logger) *JobWoker {
	return &JobWoker{kafkareader: kafka, elasticClient: elastic, logger: log.NewHelper(logger)}
}
func NewKafka(cfg *conf.Kafka) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   cfg.Brokers,
		Topic:     cfg.Topic,
		Partition: int(cfg.Partition),
	})
}
func NewElastic(cfg *conf.Elasticsearch) (*ElasticSearch, error) {

	c := elasticsearch.Config{
		Addresses: cfg.Addresses,
	}
	client, err := elasticsearch.NewTypedClient(c)
	return &ElasticSearch{
		elasticClient: client,
		index:         cfg.Index,
	}, err
}

func (js *JobWoker) Start(ctx context.Context) error {
	//kafka读取数据
	for {
		message, err := js.kafkareader.ReadMessage(ctx)
		if err != nil {
			js.logger.Errorf("kafka read message has error:%#v\n", err)
			continue
		}
		fmt.Printf("kafka read message:%#v\n", message)
		msg := new(Message)
		if err = json.Unmarshal(message.Value, msg); err != nil {
			js.logger.Errorf("json unmarshal has error :%#v\n", err)
			continue
		}
		if msg.IsDDL {
			js.logger.Errorf("message value is not ddl")
			continue
		}
		if msg.Type == "INSERT" {
			//insert
			for item := range msg.Data {
				err = js.InsertDDL(msg.Data[item], ctx)
				if err != nil {
					continue
				}
			}
		} else {
			//update
			for item := range msg.Data {
				err = js.UpdateDDL(msg.Data[item], ctx)
				if err != nil {
					continue
				}
			}
		}

	}

	return nil
}
func (js *JobWoker) Stop(context.Context) error {
	js.logger.Debug("kafka reader close")
	err := js.kafkareader.Close()
	if err != nil {
		js.logger.Errorf("kafka close reader has error:%#v\n", err)
		return err
	}
	return nil
}

func (js *JobWoker) InsertDDL(d map[string]interface{}, ctx context.Context) error {
	reveiwId := d["id"].(string)
	_, err := js.elasticClient.elasticClient.Index(js.elasticClient.index).Id(reveiwId).Document(d).Do(ctx)
	if err != nil {
		js.logger.Errorf("create docs by mysql binlog has error:%#v\n", err)
		return err
	}
	return nil
}

func (js *JobWoker) UpdateDDL(d map[string]interface{}, ctx context.Context) error {
	reveiwId := d["id"].(string)
	_, err := js.elasticClient.elasticClient.Update(js.elasticClient.index, reveiwId).Doc(d).Do(ctx)
	if err != nil {
		js.logger.Errorf("update docs by mysql binlog has error")
		return nil
	}
	return nil
}
