package database

// Search results for searches in elasticsearches.go

import "time"

// UserProblemAndVideoEventsIDsAndTime is a model for search
// result parsing
type UserProblemAndVideoEventsIDsAndTime struct {
	ProblemID string    `json:"problem_id"`
	VideoID   string    `json:"video_id"`
	Time      time.Time `json:"event_time"`
}
