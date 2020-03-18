package parsers

import (
	"encoding/json"
	"errors"
	"kafka-log-processor/pkg/models"
	"strings"
)

// ParseProblemEvent gets log object (as string represented in bytes, as it's returned
// from kafka) and returns object with problem-only-related properties.
func ParseProblemEvent(log []byte) (models.ProblemEventDescription, error) {
	var logObject models.ProblemLog
	err := json.Unmarshal(log, &logObject)
	if err != nil {
		return models.ProblemEventDescription{}, err
	}

	var problemID string

	if len(logObject.Event.Problem) > 0 {
		problemID = logObject.Event.Problem
	} else {
		problemID = logObject.Event.ProblemID
	}

	// Problem id represented as block-v1:spbu+CourseID+SessionID+type@problem+block@ProblemID
	// So we need to extract ProblemID
	if len(strings.Split(problemID, "@")) == 3 {
		problemID = strings.Split(problemID, "@")[2]
	} else {
		return models.ProblemEventDescription{}, errors.New("incorrect problem log format")
	}

	return models.ProblemEventDescription{
		EventTime:        logObject.Time,
		Username:         logObject.Username,
		ProblemID:        problemID,
		EventType:        logObject.EventType,
		WeightedEarned:   logObject.Event.WeightedEarned,
		WeightedPossible: logObject.Event.WeightedPossible,
	}, nil
}
