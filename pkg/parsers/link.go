package parsers

import (
	"encoding/json"
	"kafka-log-processor/pkg/models"
)

// ParseLinkEvent gets log object (as string represented in bytes, as it's returned
// from kafka) and returns object with links-only-related properties.
func ParseLinkEvent(log []byte) (models.LinkEventDescription, error) {
	var logObject models.LinkLog
	err := json.Unmarshal(log, &logObject)
	if err != nil {
		return models.LinkEventDescription{}, err
	}
	return models.LinkEventDescription{
		EventTime:  logObject.Time,
		Username:   logObject.Username,
		EventType:  logObject.EventType,
		CurrentURL: logObject.Event.CurrentURL,
		TargetURL:  logObject.Event.TargetURL,
		CourseID:   logObject.LinkContext.CourseID,
	}, nil
}
