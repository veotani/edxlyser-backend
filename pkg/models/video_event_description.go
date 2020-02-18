package models

// LogEventType is a string that can be in "event_type" field in edx logs
type LogEventType string

// Types of logs for video events
const (
	PlayVideo  LogEventType = "play_video"
	PauseVideo LogEventType = "pause_video"
	SeekVideo  LogEventType = "seek_video"
	StopVideo  LogEventType = "stop_video"
)

// EventType describe types for internal processing (within this system). They are mapped from
// LogEventType.
type EventType string

const (
	// PLAY means events with "play_video" event_types
	PLAY EventType = "play"
	// PAUSE means events with "pause_video", "stop_video" and "seek_video" event_types.
	// seek_video is considered to be a pause event because it doesnt provide any other information:
	// it has it's old time and new time, but the new time is also recorded in the next "play" event.
	// As a result, it is only necessary to determine the pause time, which in the "old_time" field in
	// "event" object of the event log.
	PAUSE EventType = "pause"
)

// VideoEventDescription has all the data about video events for analysis
// JSON names are also mentioned for umarshaling and sending that json to elastic
type VideoEventDescription struct {
	EventTime string    `json:"event_time"`
	VideoTime float64   `json:"video_time"`
	Username  string    `json:"username"`
	VideoID   string    `json:"video_id"`
	EventType EventType `json:"event_type"`
}
