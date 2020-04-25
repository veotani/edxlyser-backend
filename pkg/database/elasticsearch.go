package database

// ElasticSearch Service structure.
// Method `Connect` is trying to connect in an infinite loop,
// because this project uses docker and container needs some
// time to initialize.

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/olivere/elastic"
)

// Index names
const (
	CourseStructureIndexName            = "course_structure"
	VideoEventDescriptionIndexName      = "video_event_description"
	BookmarsEventDescriptionIndexName   = "bookmarks_event_description"
	LinkEventDescriptionIndexName       = "link_event_description"
	ProblemEventDescriptionIndexName    = "problem_event_description"
	SequentialEventDescriptionIndexName = "sequential_event_description"
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

func connect(host string, port int) (*elastic.Client, error) {
	for {
		elasticURL := "http://" + host + ":" + strconv.Itoa(port)
		client, err := elastic.NewClient(
			elastic.SetURL(elasticURL),
			elastic.SetRetrier(newConnectionRetrier()),
		)
		if err != nil {
			if elastic.IsConnErr(err) {
				log.Println("no elasticsearch instance avaliable, retrying connection in 5 seconds")
				time.Sleep(time.Second * 5)
				continue
			}
			return nil, err
		}
		return client, err
	}
}

// Connect to elasticsearch
func (es *ElasticService) Connect(host string, port int) error {
	client, err := connect(host, port)
	if err != nil {
		return err
	}
	es.client = client
	return nil
}
