package database

import (
	"context"
	"kafka-log-processor/pkg/models"
	"net/http"
	"strconv"
	"time"

	"github.com/olivere/elastic"
)

// ElasticService to complete all the elasticsearch requests
type ElasticService struct {
	client *elastic.Client
}

// Setting up a retry policy
type connectionRetrier struct {
	backoff elastic.Backoff
}

func newConnectionRetrier() *connectionRetrier {
	return &connectionRetrier{
		backoff: elastic.NewExponentialBackoff(3*time.Second, 5*time.Minute),
	}
}

func (r *connectionRetrier) Retry(ctx context.Context, retry int, req *http.Request, resp *http.Response, err error) (time.Duration, bool, error) { // Let the backoff strategy decide how long to wait and whether to stop
	wait, stop := r.backoff.Next(retry)
	return wait, stop, nil
}

// Connect to elasticsearch
func (es *ElasticService) Connect(host string, port int) error {
	elasticURL := "http://" + host + ":" + strconv.Itoa(port)
	client, err := elastic.NewClient(
		elastic.SetURL(elasticURL),
		elastic.SetRetrier(newConnectionRetrier()),
	)
	if err != nil {
		return err
	}

	exists, err := client.IndexExists("video_event_description").Do(context.Background())
	if err != nil {
		return err
	}
	if !exists {
		mapping := `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"event_time": { "type": "date" },
			"event_type": { "type": "keyword" },
			"username": { "type": "keyword" },
			"video_id": { "type": "keyword" },
			"video_time": { "type": "double" }
		}
	}
}
`
		_, err := client.CreateIndex("video_event_description").Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}
	es.client = client
	return nil

}

// AddVideoEventDescription adds information of a parsed log into elasticsearch
func (es ElasticService) AddVideoEventDescription(videoEventDescription models.VideoEventDescription) error {
	_, err := es.client.Index().
		Index("video_event_description").
		BodyJson(videoEventDescription).
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}
