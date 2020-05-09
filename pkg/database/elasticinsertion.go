package database

// Methods for new document insertion into ElasticSearch

import (
	"context"
	"kafka-log-processor/pkg/models"

	edxstruct "github.com/veotani/edx-structure-json"
)

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
