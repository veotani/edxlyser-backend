package database

import (
	"context"
	"kafka-log-processor/pkg/models"
	"net/http"
	"strconv"
	"time"

	"github.com/olivere/elastic"
	edxstruct "github.com/veotani/edx-structure-json"
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

	// Index for video events
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
			"video_time": { "type": "double" },
			"course_id": { "type": "keyword" }
		}
	}
}
`
		_, err := client.CreateIndex("video_event_description").Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}

	// Index for booksmark events
	exists, err = client.IndexExists("bookmarks_event_description").Do(context.Background())
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
		_, err := client.CreateIndex("bookmarks_event_description").Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}

	// Index for link events
	exists, err = client.IndexExists("link_event_description").Do(context.Background())
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
		_, err := client.CreateIndex("link_event_description").Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}

	// Index for problem events
	exists, err = client.IndexExists("problem_event_description").Do(context.Background())
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
		_, err := client.CreateIndex("problem_event_description").Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}

	// Index for sequential events
	exists, err = client.IndexExists("sequential_event_description").Do(context.Background())
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
		_, err := client.CreateIndex("sequential_event_description").Body(mapping).Do(context.Background())
		if err != nil {
			return err
		}
	}

	// Index for course structure
	exists, err = client.IndexExists("course_structure").Do(context.Background())
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
		_, err := client.CreateIndex("course_structure").Body(mapping).Do(context.Background())
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
		Index("course_structure").
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
		Index("video_event_description").
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
		Index("bookmarks_event_description").
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
		Index("link_event_description").
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
		Index("problem_event_description").
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
		Index("sequential_event_description").
		BodyJson(sequentialMoveEventDescription).
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}
