package models

// VideoLog is a definition of a log object with event type "play_video", "pause_video", "stop_video" and "seek_video"
type VideoLog struct {
	Username  string        `json:"username"`
	EventType string        `json:"event_type"`
	Time      string        `json:"time"`
	Event     VideoEventLog `json:"event"`
}

// VideoEventLog is a definition of an event object within VideoLog
type VideoEventLog struct {
	CurrentTime float64 `json:"currentTime"`
	OldTime     float64 `json:"old_time"`
	ID          string  `json:"id"`
}
