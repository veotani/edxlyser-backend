package models

import (
	"fmt"
	"strings"
)

// GetCourseCodeFromCourseID extracts CourseCode from "course-v1:org+CourseCode+CourseRun" notation
func GetCourseCodeFromCourseID(courseID string) (string, error) {
	courseIDSplited := strings.Split(courseID, "+")
	if len(courseIDSplited) != 3 {
		return "", fmt.Errorf("Course ID should have 3 sections separated by \"+\" symbol, got course_id = \"%v\"", courseID)
	}
	return courseIDSplited[1], nil
}
