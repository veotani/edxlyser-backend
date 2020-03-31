package models

// SequentialMoveEventDescription has all the data about moving within sequential object
type SequentialMoveEventDescription struct {
	EventTime string `json:"event_time"`
	Username  string `json:"username"`
	Old       int    `json:"old"`
	EventType string `json:"event_type"`
	New       int    `json:"new"`
}
