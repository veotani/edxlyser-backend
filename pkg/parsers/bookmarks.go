package parsers

import (
	"encoding/json"
	"kafka-log-processor/pkg/models"
)

// ParseBookmarksEvent gets log object (as string represented in bytes, as it's returned
// from kafka) and returns object with sequential-only-related properties.
func ParseBookmarksEvent(log []byte) (models.BookmarksEventDescription, error) {
	var logObject models.BookmarksLog
	err := json.Unmarshal(log, &logObject)
	if err != nil {
		return models.BookmarksEventDescription{}, err
	}
	isAdded := logObject.EventType == "edx.bookmark.added"
	return models.BookmarksEventDescription{
		EventTime: logObject.Time,
		Username:  logObject.Username,
		EventType: logObject.EventType,
		ID:        logObject.Event.ComponentUsageID,
		IsAdded:   isAdded,
	}, nil
}
