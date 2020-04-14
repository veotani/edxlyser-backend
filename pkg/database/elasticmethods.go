package database

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"

	"github.com/olivere/elastic"
	edxstructure "github.com/veotani/edx-structure-json"
)

// GetUniqueStringFieldValuesInIndex returns all possible values of field fieldName in index indexName
// Field fieldName should have type keyword (string)
func (es *ElasticService) GetUniqueStringFieldValuesInIndex(indexName string, fieldName string) ([]string, error) {
	if es.client == nil {
		return nil, errors.New("You need to connect to ElasticSearch first")
	}
	searchResults, err := es.client.Search().
		Index(indexName).
		Aggregation("custom_aggregation", elastic.NewTermsAggregation().
			Field(fieldName)).
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	aggregationResults, ok := searchResults.Aggregations.Terms("custom_aggregation")
	if !ok {
		return nil, errors.New("Nothing was found")
	}
	result := make([]string, 0)
	for _, bucket := range aggregationResults.Buckets {
		if fieldValue, ok := bucket.Key.(string); ok {
			result = append(result, fieldValue)
		} else {
			return nil, fmt.Errorf("Field %v should be a keyword", fieldName)
		}
	}
	return result, nil
}

// GetUniqueStringFieldValuesInIndexWithFilter is a GetUniqueStringFieldValuesInIndex but with ability to filter the field filteredFieldName
func (es *ElasticService) GetUniqueStringFieldValuesInIndexWithFilter(indexName string, fieldName string, filteredFieldName string, filteredFieldValue interface{}) ([]string, error) {
	if es.client == nil {
		return nil, errors.New("You need to connect to ElasticSearch first")
	}
	searchResults, err := es.client.Search().
		Index(indexName).
		Query(elastic.NewTermQuery(filteredFieldName, filteredFieldValue)).
		Aggregation("custom_aggregation", elastic.NewTermsAggregation().
			Field(fieldName).
			Size(1e8)).
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	aggregationResults, ok := searchResults.Aggregations.Terms("custom_aggregation")
	if !ok {
		return nil, errors.New("Nothing was found")
	}
	result := make([]string, 0)
	for _, bucket := range aggregationResults.Buckets {
		if fieldValue, ok := bucket.Key.(string); ok {
			result = append(result, fieldValue)
		} else {
			return nil, fmt.Errorf("Field %v should be a keyword", fieldName)
		}
	}
	return result, nil
}

// GetCourseStructure gets structure for course with course code = courseCode and course run = courseRun
func (es *ElasticService) GetCourseStructure(courseCode string) (edxstructure.Course, error) {
	searchResults, err := es.client.
		Search().
		Index(CourseStructureIndexName).
		Query(elastic.NewTermQuery("course_code.keyword", courseCode)).
		Do(context.Background())
	if err != nil {
		return edxstructure.Course{}, err
	}
	var course edxstructure.Course
	for _, course := range searchResults.Each(reflect.TypeOf(course)) {
		return course.(edxstructure.Course), nil
	}
	return edxstructure.Course{}, fmt.Errorf("No structures for course %v was found", courseCode)
}

// GetUserVideoAndProblemEventsTimes gets all video events and problem events logs for user with username and gets
// their IDs and timestamps
func (es *ElasticService) GetUserVideoAndProblemEventsTimes(username string, courseID string) ([]UserProblemAndVideoEventsIDsAndTime, error) {
	res, err := es.client.
		Search().
		Index(VideoEventDescriptionIndexName).
		Index(ProblemEventDescriptionIndexName).
		Query(elastic.NewBoolQuery().Must(
			elastic.NewTermQuery("username", username),
			elastic.NewTermQuery("course_id", courseID),
		)).
		Size(1e4).
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	if res.Hits.TotalHits.Relation == "gte" {
		log.Println("WARN: Query doesnt get all the documents at the moment. It's time to add iterating over pages.")
	}
	var a UserProblemAndVideoEventsIDsAndTime

	videoAndProblemIDsAdnEventTimes := make([]UserProblemAndVideoEventsIDsAndTime, 0)
	for _, r := range res.Each(reflect.TypeOf(a)) {
		result, ok := r.(UserProblemAndVideoEventsIDsAndTime)
		if !ok {
			log.Println("WARN: error while converting search results")
		} else {
			videoAndProblemIDsAdnEventTimes = append(videoAndProblemIDsAdnEventTimes, result)
		}
	}
	return videoAndProblemIDsAdnEventTimes, nil
}
