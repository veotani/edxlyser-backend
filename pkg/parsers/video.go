package parsers

import (
	"encoding/json"
	"errors"
	"fmt"
	"kafka-log-processor/pkg/helpers"
	"kafka-log-processor/pkg/models"
)

// ParseVideoEvent gets log object (as string represented in bytes, as it's returned
// from kafka) and returns object with video-only-related properties.
func ParseVideoEvent(log []byte) (models.VideoEventDescription, error) {
	videoLog, err := logBytesToMap(log)
	if err != nil {
		return models.VideoEventDescription{}, err
	}

	logType, err := determineLogType(videoLog)
	if err != nil {
		return models.VideoEventDescription{}, err
	}

	var videoEventDescription models.VideoEventDescription
	switch logType {
	case models.PlayVideo:
		videoEventDescription, err = parsePlayVideoEvent(videoLog)
	case models.PauseVideo:
		videoEventDescription, err = parsePauseVideoEvent(videoLog)
	case models.StopVideo:
		videoEventDescription, err = parseStopVideoEvent(videoLog)
	case models.SeekVideo:
		videoEventDescription, err = parseSeekVideoEvent(videoLog)
	default:
		return models.VideoEventDescription{}, errors.New("Unexpected log type")
	}

	if err != nil {
		return models.VideoEventDescription{}, err
	}

	return videoEventDescription, nil
}

func logBytesToMap(log []byte) (map[string]interface{}, error) {
	var logObject interface{}
	json.Unmarshal(log, &logObject)

	decodedLogObject, ok := logObject.(map[string]interface{})

	if !ok {
		return nil, fmt.Errorf("Error in parsing log %v", string(log))
	}

	return decodedLogObject, nil
}

func determineLogType(log map[string]interface{}) (models.LogEventType, error) {
	eventTypeObject, ok := log["event_type"]
	if !ok {
		return "", errors.New("no event type in the log")
	}
	eventType, ok := eventTypeObject.(string)
	if !ok {
		return "", errors.New("event_type is not a string")
	}

	switch eventType {
	case "play_video":
		return models.PlayVideo, nil
	case "pause_video":
		return models.PauseVideo, nil
	case "seek_video":
		return models.SeekVideo, nil
	case "stop_video":
		return models.StopVideo, nil
	default:
		return "", errors.New("unexpected event type")
	}
}

func parsePlayVideoEvent(log map[string]interface{}) (models.VideoEventDescription, error) {
	// Get "event" field
	eventObject, ok := log["event"]
	if !ok {
		return models.VideoEventDescription{}, errors.New("no event in the log")
	}
	event, ok := eventObject.(map[string]interface{})
	if !ok {
		return models.VideoEventDescription{}, errors.New("log event is not an object")
	}

	// Get "username" field
	username, err := helpers.ExtractStringFieldFromGenericMap(log, "username")
	if err != nil {
		return models.VideoEventDescription{}, err
	}

	// Get "time" field
	eventTime, err := helpers.ExtractStringFieldFromGenericMap(log, "time")
	if err != nil {
		return models.VideoEventDescription{}, err
	}

	// Get "id" field
	videoID, err := helpers.ExtractStringFieldFromGenericMap(event, "id")
	if err != nil {
		return models.VideoEventDescription{}, err
	}

	// Get "currentTime" field
	videoTimeObject, ok := event["currentTime"]
	if !ok {
		return models.VideoEventDescription{}, errors.New("no currentTime in the log")
	}
	videoTime, ok := videoTimeObject.(float64)
	if !ok {
		return models.VideoEventDescription{}, errors.New("log currentTime is not an float64")
	}

	videoEventDescription := models.VideoEventDescription{
		EventTime: eventTime,
		VideoTime: videoTime,
		Username:  username,
		VideoID:   videoID,
		EventType: models.PLAY,
	}

	return videoEventDescription, nil
}

func parsePauseVideoEvent(log map[string]interface{}) (models.VideoEventDescription, error) {
	// Play and Pause video events have same structure
	videoEventDescription, err := parsePlayVideoEvent(log)
	if err != nil {
		return models.VideoEventDescription{}, err
	}
	videoEventDescription.EventType = models.PAUSE
	return videoEventDescription, nil
}

func parseSeekVideoEvent(log map[string]interface{}) (models.VideoEventDescription, error) {
	// Get "event" field
	eventObject, ok := log["event"]
	if !ok {
		return models.VideoEventDescription{}, errors.New("no event in the log")
	}
	event, ok := eventObject.(map[string]interface{})
	if !ok {
		return models.VideoEventDescription{}, errors.New("log event is not an object")
	}

	// Get "username" field
	username, err := helpers.ExtractStringFieldFromGenericMap(log, "username")
	if err != nil {
		return models.VideoEventDescription{}, err
	}

	// Get "time" field
	eventTime, err := helpers.ExtractStringFieldFromGenericMap(log, "time")
	if err != nil {
		return models.VideoEventDescription{}, err
	}

	// Get "id" field
	videoID, err := helpers.ExtractStringFieldFromGenericMap(event, "id")
	if err != nil {
		return models.VideoEventDescription{}, err
	}

	// Get "currentTime" field
	videoTimeObject, ok := event["new_time"]
	if !ok {
		return models.VideoEventDescription{}, errors.New("no currentTime in the log")
	}
	videoTime, ok := videoTimeObject.(float64)
	if !ok {
		return models.VideoEventDescription{}, errors.New("log currentTime is not an float64")
	}

	videoEventDescription := models.VideoEventDescription{
		EventTime: eventTime,
		VideoTime: videoTime,
		Username:  username,
		VideoID:   videoID,
		EventType: models.PAUSE,
	}

	return videoEventDescription, nil
}

func parseStopVideoEvent(log map[string]interface{}) (models.VideoEventDescription, error) {
	// Play and Stop video events have same structure
	videoEventDescription, err := parsePlayVideoEvent(log)
	if err != nil {
		return models.VideoEventDescription{}, err
	}
	videoEventDescription.EventType = models.PAUSE

	return videoEventDescription, nil
}
