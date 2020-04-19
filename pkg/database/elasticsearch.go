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

	// Index for video events
	exists, err := client.IndexExists(VideoEventDescriptionIndexName).Do(context.Background())
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
			"video_time": { "type": "double" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := client.CreateIndex(VideoEventDescriptionIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}

	// Index for booksmark events
	exists, err = client.IndexExists(BookmarsEventDescriptionIndexName).Do(context.Background())
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
			"id": { "type": "keyword" },
			"is_added": { "type": "binary" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := client.CreateIndex(BookmarsEventDescriptionIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}

	// Index for link events
	exists, err = client.IndexExists(LinkEventDescriptionIndexName).Do(context.Background())
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
			"current_url": { "type": "keyword" },
			"target_url": { "type": "keyword" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := client.CreateIndex(LinkEventDescriptionIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}

	// Index for problem events
	exists, err = client.IndexExists(ProblemEventDescriptionIndexName).Do(context.Background())
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
			"problem_id": { "type": "keyword" },
			"weighted_earned": { "type": "double" },
			"weighted_possible": { "type": "double" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := client.CreateIndex(ProblemEventDescriptionIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}

	// Index for sequential events
	exists, err = client.IndexExists(SequentialEventDescriptionIndexName).Do(context.Background())
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
			"old": { "type": "integer" },
			"new": { "type": "integer" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := client.CreateIndex(SequentialEventDescriptionIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}

	// Index for course structure
	exists, err = client.IndexExists(CourseStructureIndexName).Do(context.Background())
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
			"course": {
				"properties": {
					"display_name": {"type": "keyword"},
					"course_code": {"type": "keyword"},
					"course_run": {"type": "keyword"},
					"chapters": {
						"properties": {
							"url_name": {"type": "keyword"},
							"display_name": {"type": "keyword"},
							"sequentials": {
								"properties": {
									"display_name": {"type": "keyword"},
									"url_name": {"type": "keyword"},
									"verticals": {
										"properties": {
											"display_name": {"type": "keyword"},
											"url_name": {"type": "keyword"},
											"problems": {
												"properties": {
													"display_name": {"type": "keyword"},
													"url_name": {"type": "keyword"}
												}
											},
											"discussions": {
												"properties": {
													"url_name": {"type": "keyword"}
												}
											},
											"htmls": {
												"properties": {
													"display_name": {"type": "keyword"},
													"url_name": {"type": "keyword"}
												}
											},
											"open_assessments": {
												"properties": {
													"url_name": {"type": "keyword"}
												}
											},
											"library_contents": {
												"properties": {
													"display_name": {"type": "keyword"},
													"url_name": {"type": "keyword"},
													"problems": {
														"properties": {
															"display_name": {"type": "keyword"},
															"url_name": {"type": "keyword"}
														}
													}
												}
											},
											"videos": {
												"properties": {
													"display_name": {"type": "keyword"},
													"url_name": {"type": "keyword"},
													"duration": {"type": "keyword"}
												}
											}
										}
									}
								}
							}
						}
					}
				} 
			}
		}
	}
}
`
		_, err := client.CreateIndex(CourseStructureIndexName).Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
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
