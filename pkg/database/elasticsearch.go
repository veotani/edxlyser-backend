package database

import (
	"context"
	"kafka-log-processor/pkg/models"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/olivere/elastic"
	edxstruct "github.com/veotani/edx-structure-json"
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

// AddCourseStructure adds information of course structure
func (es ElasticService) AddCourseStructure(course edxstruct.Course) error {
	_, err := es.client.Index().
		Index(CourseStructureIndexName).
		BodyJson(course).
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// AddVideoEventDescription adds information of a parsed log into elasticsearch
func (es ElasticService) AddVideoEventDescription(videoEventDescription models.VideoEventDescription) error {
	_, err := es.client.Index().
		Index(VideoEventDescriptionIndexName).
		BodyJson(videoEventDescription).
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// AddBooksmarkEventDescription adds information of a parsed log into elasticsearch
func (es ElasticService) AddBooksmarkEventDescription(booksmarkEventDescription models.BookmarksEventDescription) error {
	_, err := es.client.Index().
		Index(BookmarsEventDescriptionIndexName).
		BodyJson(booksmarkEventDescription).
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// AddLinkEventDescription adds information of a parsed log into elasticsearch
func (es ElasticService) AddLinkEventDescription(linkEventDescription models.LinkEventDescription) error {
	_, err := es.client.Index().
		Index(LinkEventDescriptionIndexName).
		BodyJson(linkEventDescription).
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// AddProblemEventDescription adds information of a parsed log into elasticsearch
func (es ElasticService) AddProblemEventDescription(problemEventDescription models.ProblemEventDescription) error {
	_, err := es.client.Index().
		Index(ProblemEventDescriptionIndexName).
		BodyJson(problemEventDescription).
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// AddSequentialMoveEventDescription adds information of a parsed log into elasticsearch
func (es ElasticService) AddSequentialMoveEventDescription(sequentialMoveEventDescription models.SequentialMoveEventDescription) error {
	_, err := es.client.Index().
		Index(SequentialEventDescriptionIndexName).
		BodyJson(sequentialMoveEventDescription).
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}
