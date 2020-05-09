package analysers

import (
	"errors"
	"fmt"
	"kafka-log-processor/pkg/database"
	"kafka-log-processor/pkg/models"
	"log"
	"strings"
)

// GetCourseUsersRoute returns points for plot that shows users route on specified course.
// course must have format: "course-v1:org+CourseCode+CourseRun"
func (a *Analyser) GetCourseUsersRoute(course string) ([]models.Curve, error) {
	videoUsernames, err := a.elasticService.GetUniqueStringFieldValuesInIndexWithFilter(database.VideoEventDescriptionIndexName, "username", "course_id", course)
	if err != nil {
		return nil, err
	}

	problemUsernames, err := a.elasticService.GetUniqueStringFieldValuesInIndexWithFilter(database.ProblemEventDescriptionIndexName, "username", "course_id", course)
	if err != nil {
		return nil, err
	}

	usernames := append(videoUsernames, problemUsernames...)
	fmt.Printf("usernames:%v", len(usernames))
	itemOrdersMap, err := a.getItemOrdersMap(course)
	if err != nil {
		return nil, err
	}

	curves := []models.Curve{}

	for _, username := range usernames {
		X := make([]int, 0)
		Y := make([]int, 0)
		actionNumber := 0
		userActions, err := a.elasticService.GetUserVideoAndProblemEventsTimes(username, course)
		if err != nil {
			return nil, err
		}
		prevActionID := ""
		for _, action := range userActions {
			if action.ProblemID != "" {
				if action.ProblemID != prevActionID {
					problemOrder, ok := itemOrdersMap[action.ProblemID]
					if !ok {
						log.Printf("WARN: couldn't get order of item with ID %v\n", action.ProblemID)
						continue
					}
					X = append(X, problemOrder)
					Y = append(Y, actionNumber)
					actionNumber++
					prevActionID = action.ProblemID
				}
			}

			if action.VideoID != "" {
				if action.VideoID != prevActionID {
					videoOrder, ok := itemOrdersMap[action.VideoID]
					if !ok {
						log.Printf("WARN: couldn't get order of item with ID %v\n", action.VideoID)
						continue
					}
					X = append(X, videoOrder)
					Y = append(Y, actionNumber)
					actionNumber++
					prevActionID = action.VideoID
				}
			}
		}

		curves = append(curves, models.Curve{X: X, Y: Y})
	}
	return curves, nil
}

func (a *Analyser) getItemOrdersMap(course string) (map[string]int, error) {
	courseIDSplit := strings.Split(course, "+")
	if len(courseIDSplit) < 3 {
		return nil, errors.New("CourseID had incorrect format")
	}

	result := map[string]int{}
	currentItemNumber := 0
	courseCode := courseIDSplit[1]

	courseStructure, err := a.elasticService.GetCourseStructure(courseCode)
	if err != nil {
		return nil, err
	}

	for _, chapter := range courseStructure.Chapters {
		for _, sequential := range chapter.Sequentials {
			for _, vertical := range sequential.Verticals {
				for _, video := range vertical.Videos {
					result[video.URLName] = currentItemNumber
					currentItemNumber++
				}
				for _, problem := range vertical.Problems {
					result[problem.URLName] = currentItemNumber
					currentItemNumber++
				}
			}
		}
	}

	return result, nil
}
