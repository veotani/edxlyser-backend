package database

import "time"

type UserProblemAndVideoEventsIDsAndTime struct {
	ProblemID string    `json:"problem_id"`
	VideoID   string    `json:"video_id"`
	Time      time.Time `json:"event_time"`
}
