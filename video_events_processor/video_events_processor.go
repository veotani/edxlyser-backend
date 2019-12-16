package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/olivere/elastic"
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
	EventTime string    `json:"event_time"`
	VideoTime float64   `json:"video_time"`
	Username  string    `json:"username"`
	VideoID   string    `json:"video_id"`
	EventType EventType `json:"event_type"`
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
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
		EventTime: eventTime,
		VideoTime: videoTime,
		Username:  username,
		VideoID:   videoID,
		EventType: PLAY,
	}

	return videoEventDescription, nil
}

func parsePauseVideoEvent(log map[string]interface{}) (VideoEventDescription, error) {
	// Play and Pause video events have same structure
	videoEventDescription, err := parsePlayVideoEvent(log)
	if err != nil {
		return VideoEventDescription{}, err
	}
	videoEventDescription.EventType = PAUSE

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
		EventTime: eventTime,
		VideoTime: videoTime,
		Username:  username,
		VideoID:   videoID,
		EventType: PAUSE,
	}

	return videoEventDescription, nil
}

func parseStopVideoEvent(log map[string]interface{}) (VideoEventDescription, error) {
	// Play and Stop video events have same structure
	videoEventDescription, err := parsePlayVideoEvent(log)
	if err != nil {
		return VideoEventDescription{}, err
	}
	videoEventDescription.EventType = PAUSE

	return videoEventDescription, nil
}

// Run process of getting logs from kafka, parsing them and putting to elastic
func Run() {
	kafkaCon := getKafkaConnection()
	elasticCon, err := getElasticConUntilSuccess()
	if err != nil {
		panic(err)
	}
	countBadLogs := 0
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
				videoEventDescription.EventTime,
				videoEventDescription.EventType,
				videoEventDescription.Username,
				videoEventDescription.VideoID,
				videoEventDescription.VideoTime,
			)
			countBadLogs++
			fmt.Println(countBadLogs)
			time.Sleep(time.Second)
		} else {
			log.Printf(
				"New log: %v, %v, %v, %v, %v\n",
				videoEventDescription.EventTime,
				videoEventDescription.EventType,
				videoEventDescription.Username,
				videoEventDescription.VideoID,
				videoEventDescription.VideoTime,
			)
			addToElastic(videoEventDescription, elasticCon)
		}

	}
}

func isInitialisedWithDefaultValues(videoEventDescription VideoEventDescription) bool {
	if videoEventDescription.EventTime == "" || videoEventDescription.VideoTime == -1 ||
		videoEventDescription.Username == "" || videoEventDescription.VideoID == "" {
		return true
	}
	return false
}

func getElasticConUntilSuccess() (*elastic.Client, error) {
	for {
		elasticCon, err := getElassticCon()
		if err != nil {
			log.Println(err)
			continue
		}
		return elasticCon, nil
	}
}

func getElassticCon() (*elastic.Client, error) {
	elasticURL := "http://localhost:9200"
	client, err := elastic.NewClient(elastic.SetURL(elasticURL))
	if err != nil {
		return nil, err
	}

	exists, err := client.IndexExists("video_event_description").Do(context.Background())
	if err != nil {
		return nil, err
	}
	if !exists {
		mapping := `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"properties":{
			"event_time": { "type": "date" },
			"event_type": { "type": "keyword" },
			"username": { "type": "keyword" },
			"video_id": { "type": "keyword" },
			"video_time": { "type": "double" }
		}
	}
}
`
		_, err := client.CreateIndex("video_event_description").Body(mapping).Do(context.Background())
		if err != nil {
			return nil, err
		}
	}
	return client, nil
}

func addToElastic(videoEventDescription VideoEventDescription, client *elastic.Client) error {
	_, err := client.Index().
		Index("video_event_description").
		BodyJson(videoEventDescription).
		Do(context.Background())
	if err != nil {
		return err
	}
	return nil
}
