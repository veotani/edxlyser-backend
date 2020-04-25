package database

// Saved searches in ElasticSearch used in this project

import (
	"context"
	"errors"
	"fmt"
	"kafka-log-processor/pkg/models"
	"log"
	"reflect"

	"github.com/olivere/elastic"
	edxparser "github.com/veotani/edx-structure-json"
	edxstructure "github.com/veotani/edx-structure-json"
)

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

// GetUserVideoEvents returns all video events for specific user and a video
func (es *ElasticService) GetUserVideoEvents(username string, videoID string) ([]models.VideoEventDescription, error) {
	res, err := es.client.
		Search().
		Index(VideoEventDescriptionIndexName).
		Query(elastic.NewBoolQuery().Must(
			elastic.NewTermQuery("username", username),
			elastic.NewTermQuery("video_id", videoID),
		)).
		Size(1e4).
		Do(
			context.Background(),
		)
	if err != nil {
		return nil, err
	}
	if res.Hits.TotalHits.Relation == "gte" {
		log.Println("WARN: Query doesnt get all the documents at the moment. It's time to add iterating over pages.")
	}

	result := make([]models.VideoEventDescription, 0)
	var videoEventDescription models.VideoEventDescription
	for _, r := range res.Each(reflect.TypeOf(videoEventDescription)) {
		videoEventDescription, ok := r.(models.VideoEventDescription)
		if !ok {
			log.Println("WARN: error while converting search results")
		} else {
			result = append(result, videoEventDescription)
		}
	}

	return result, nil
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

// GetAllCourseCodesWithStructure returns all possible course_code values in course structures index
func (es *ElasticService) GetAllCourseCodesWithStructure() ([]string, error) {
	if es.client == nil {
		return nil, errors.New("You need to connect to ElasticSearch first")
	}
	searchResult, err := es.client.
		Search().
		Index(CourseStructureIndexName).
		Query(elastic.NewMatchAllQuery()).
		Do(context.Background())
	if err != nil {
		return nil, err
	}

	result := make([]string, 0)
	var course edxparser.Course
	for _, c := range searchResult.Each(reflect.TypeOf(course)) {
		if currentCourse, ok := c.(edxparser.Course); ok {
			result = append(result, currentCourse.CourseCode)
		} else {
			return nil, errors.New("Couldn't parse course structure index")
		}
	}

	return result, nil
}

// GetAllCourseIDsWithStructureAndLogs gets all course ids met in logs, then
// scans for courses in structure index and returns their union.
// The returned type is in "course-v1:org+CourseCode+CourseRun" notation.
func (es *ElasticService) GetAllCourseIDsWithStructureAndLogs() ([]string, error) {
	courseStructureCourses, err := es.GetAllCourseCodesWithStructure()
	if err != nil {
		return nil, err
	}
	videoEventsCourses, err := es.GetUniqueStringFieldValuesInIndex(VideoEventDescriptionIndexName, "course_id")
	if err != nil {
		return nil, err
	}
	problemEventsCourses, err := es.GetUniqueStringFieldValuesInIndex(ProblemEventDescriptionIndexName, "course_id")
	if err != nil {
		return nil, err
	}

	log.Println(courseStructureCourses)
	log.Println(videoEventsCourses)

	result := make([]string, 0)

	for _, videoEventsCourse := range videoEventsCourses {
		for _, coursecourseStructureCourse := range courseStructureCourses {
			videoEventsCourseCode, err := models.GetCourseCodeFromCourseID(videoEventsCourse)
			if err != nil {
				log.Println("Skipping video event because it had invalid course_id. Please check the data!")
				continue
			}
			if coursecourseStructureCourse == videoEventsCourseCode {
				result = append(result, videoEventsCourse)
			}
		}
	}

	for _, problemEventsCourse := range problemEventsCourses {
		for _, coursecourseStructureCourse := range courseStructureCourses {
			problemEventsCourseCode, err := models.GetCourseCodeFromCourseID(problemEventsCourse)
			if err != nil {
				log.Println("Skipping problem event because it had invalid course_id. Please check the data!")
				continue
			}
			if coursecourseStructureCourse == problemEventsCourseCode {
				result = append(result, problemEventsCourse)
			}
		}
	}

	return result, nil
}
