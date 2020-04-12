package parsers

import (
	"encoding/json"
	"kafka-log-processor/pkg/models"
)

// ParseSequentialEvent gets log object (as string represented in bytes, as it's returned
// from kafka) and returns object with sequential-only-related properties.
func ParseSequentialEvent(log []byte) (models.SequentialMoveEventDescription, error) {
	var logObject models.SequentialLog
	err := json.Unmarshal(log, &logObject)
	if err != nil {
		return models.SequentialMoveEventDescription{}, err
	}
	return models.SequentialMoveEventDescription{
		EventTime: logObject.Time,
		Username:  logObject.Username,
		EventType: logObject.EventType,
		New:       logObject.Event.New,
		Old:       logObject.Event.Old,
		CourseID:  logObject.SequentialContext.CourseID,
	}, nil
}
