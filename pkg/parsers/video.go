package parsers

import (
	"encoding/json"
	"kafka-log-processor/pkg/models"
)

// ParseVideoEvent gets log object (as string represented in bytes, as it's returned
// from kafka) and returns object with video-only-related properties.
func ParseVideoEvent(log []byte) (models.VideoEventDescription, error) {
	var logObject models.VideoLog
	err := json.Unmarshal(log, &logObject)
	if err != nil {
		return models.VideoEventDescription{}, err
	}

	if logObject.EventType == "play_video" {
		return models.VideoEventDescription{
			EventTime: logObject.Time,
			Username:  logObject.Username,
			EventType: models.PLAY,
			VideoID:   logObject.Event.ID,
			VideoTime: logObject.Event.CurrentTime,
		}, nil
	}

	if logObject.EventType == "seek_video" {
		return models.VideoEventDescription{
			EventTime: logObject.Time,
			Username:  logObject.Username,
			EventType: models.PAUSE,
			VideoID:   logObject.Event.ID,
			VideoTime: logObject.Event.OldTime,
		}, nil
	}

	return models.VideoEventDescription{
		EventTime: logObject.Time,
		Username:  logObject.Username,
		EventType: models.PAUSE,
		VideoID:   logObject.Event.ID,
		VideoTime: logObject.Event.CurrentTime,
	}, nil
}
