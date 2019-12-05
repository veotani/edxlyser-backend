package videoeventsprocessor

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip" // gzip is a package for log decompression
)

/*
EventType describe types for internal processing. While logs have theese four types:
	- "play_video";
	- "pause_video";
	- "stop_video";
	- "seek_video".
They are considered to be equal for analysis tools. This is why equal events must
be mapped to another types. See the mappings in the constants below.
*/
type EventType string

// LogEventType is a string that can be in "event_type" field in edx logs
type LogEventType string

const (
	// PLAY means "play_video" events
	PLAY EventType = "play"
	// PAUSE means "pause_video", "stop_video" and "seek_video" events
	PAUSE EventType = "pause"
)

// Types of logs for video events
const (
	PlayVideo  LogEventType = "play_video"
	PauseVideo LogEventType = "pause_video"
	SeekVideo  LogEventType = "seek_video"
	StopVideo  LogEventType = "stop_video"
)

// VideoEventDescription has all the data about video events for analysis
type VideoEventDescription struct {
	eventTime string
	videoTime float64
	username  string
	videoID   string
	eventType EventType
}

func determineType(log map[string]interface{}) (LogEventType, error) {
	for k, v := range log {
		if k == "event_type" {
			switch v.(type) {
			case string:
				switch v {
				case "play_video":
					return PlayVideo, nil
				case "pause_video":
					return PauseVideo, nil
				case "seek_video":
					return SeekVideo, nil
				case "stop_video":
					return StopVideo, nil
				default:
					return "", errors.New("unexpected event type")
				}
			default:
				return "", errors.New("cant read event type")
			}
		}
	}
	return "", errors.New("no event type in the log")
}

// func extractVideoEventDescriptionFromLog(log []byte) {
// 	var logAsGenericObject interface{}
// 	json.Unmarshal(log, &logAsGenericObject)
// }

func getNextEvent(connection *kafka.Reader) (map[string]interface{}, error) {
	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)
	m, err := connection.ReadMessage(ctx)
	if err != nil {
		return nil, err
	}

	var logObject interface{}
	json.Unmarshal(m.Value, &logObject)

	decodedLogObject, ok := logObject.(map[string]interface{})

	if !ok {
		log.Println("not a json object")
	}
	/*
		PLEASE DELETE EVERYTHING AFTER THIS
	*/

	/*
		PLEASE DELETE EVERYTHING BEFORE THIS
	*/
	return decodedLogObject, nil
}

func getKafkaConnection() *kafka.Reader {
	connection := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "VideoEvents",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e7, // 10MB
	})
	return connection
}

func parsePlayVideoEvent(log map[string]interface{}) (VideoEventDescription, error) {
	var (
		eventTime string  = ""
		videoTime float64 = -1
		username  string  = ""
		videoID   string  = ""

		event map[string]interface{}
		ok    bool
	)

	for k, v := range log {
		switch k {
		case "event":
			event, ok = v.(map[string]interface{})
			if !ok {
				return VideoEventDescription{}, errors.New("log event is not an object")
			}
		case "username":
			username, ok = v.(string)
			if !ok {
				return VideoEventDescription{}, errors.New("log username is not a string")
			}
		case "time":
			eventTime, ok = v.(string)
			if !ok {
				return VideoEventDescription{}, errors.New("log time is not a string")
			}
		}
	}

	for k, v := range event {
		switch k {
		case "id":
			videoID, ok = v.(string)
			if !ok {
				return VideoEventDescription{}, errors.New("log event videoID is not a string")
			}
		case "currentTime":
			videoTime, ok = v.(float64)
			if !ok {
				return VideoEventDescription{}, errors.New("log event currentTime is not a float64")
			}
		}
	}

	videoEventDescription := VideoEventDescription{
		eventTime: eventTime,
		videoTime: videoTime,
		username:  username,
		videoID:   videoID,
		eventType: PLAY,
	}

	return videoEventDescription, nil
}

func parsePauseVideoEvent(log map[string]interface{}) (VideoEventDescription, error) {
	// Play and Pause video events have same structure
	videoEventDescription, err := parsePlayVideoEvent(log)
	if err != nil {
		return VideoEventDescription{}, err
	}
	videoEventDescription.eventType = PAUSE

	return videoEventDescription, nil
}

func parseSeekVideoEvent(log map[string]interface{}) (VideoEventDescription, error) {
	var (
		eventTime string  = ""
		videoTime float64 = -1
		username  string  = ""
		videoID   string  = ""

		event map[string]interface{}
		ok    bool
	)

	for k, v := range log {
		switch k {
		case "event":
			event, ok = v.(map[string]interface{})
			if !ok {
				return VideoEventDescription{}, errors.New("log event is not an object")
			}
		case "username":
			username, ok = v.(string)
			if !ok {
				return VideoEventDescription{}, errors.New("log username is not a string")
			}
		case "time":
			eventTime, ok = v.(string)
			if !ok {
				return VideoEventDescription{}, errors.New("log time is not a string")
			}
		}
	}

	for k, v := range event {
		switch k {
		case "id":
			videoID, ok = v.(string)
			if !ok {
				return VideoEventDescription{}, errors.New("log event videoID is not a string")
			}
		case "new_time":
			videoTime, ok = v.(float64)
			if !ok {
				return VideoEventDescription{}, errors.New("log event currentTime is not a float64")
			}
		}
	}

	videoEventDescription := VideoEventDescription{
		eventTime: eventTime,
		videoTime: videoTime,
		username:  username,
		videoID:   videoID,
		eventType: PAUSE,
	}

	return videoEventDescription, nil
}

func parseStopVideoEvent(log map[string]interface{}) (VideoEventDescription, error) {
	// Play and Stop video events have same structure
	videoEventDescription, err := parsePlayVideoEvent(log)
	if err != nil {
		return VideoEventDescription{}, err
	}
	videoEventDescription.eventType = PAUSE

	return videoEventDescription, nil
}

// Run process of getting logs from kafka, parsing them and putting to elastic
func Run() {
	kafkaCon := getKafkaConnection()
	for {
		eventLog, err := getNextEvent(kafkaCon)
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
		}

		logType, err := determineType(eventLog)
		if err != nil {
			log.Println(err)
		}

		var videoEventDescription VideoEventDescription
		switch logType {
		case PlayVideo:
			videoEventDescription, err = parsePlayVideoEvent(eventLog)
		case PauseVideo:
			videoEventDescription, err = parsePauseVideoEvent(eventLog)
		case StopVideo:
			videoEventDescription, err = parseStopVideoEvent(eventLog)
		case SeekVideo:
			videoEventDescription, err = parseSeekVideoEvent(eventLog)
		default:
			log.Println("Unexpected log type")
			continue
		}

		if err != nil {
			log.Println(err)
			time.Sleep(time.Second) // DEBUG
		}

		// TODO: if isInitialisedWithDefaultValues => dont send to elastic
		if isInitialisedWithDefaultValues(videoEventDescription) {
			log.Printf(
				"Bad log: %v, %v, %v, %v, %v\n",
				videoEventDescription.eventTime,
				videoEventDescription.eventType,
				videoEventDescription.username,
				videoEventDescription.videoID,
				videoEventDescription.videoTime,
			)
			time.Sleep(time.Hour)
		} else {
			log.Printf(
				"New log: %v, %v, %v, %v, %v\n",
				videoEventDescription.eventTime,
				videoEventDescription.eventType,
				videoEventDescription.username,
				videoEventDescription.videoID,
				videoEventDescription.videoTime,
			)
		}

	}
}

func isInitialisedWithDefaultValues(videoEventDescription VideoEventDescription) bool {
	if videoEventDescription.eventTime == "" || videoEventDescription.videoTime == -1 ||
		videoEventDescription.username == "" || videoEventDescription.videoID == "" {
		return true
	}
	return false
}
