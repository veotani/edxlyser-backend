package models

// BookmarksEventDescription is an bookmark creation event
// IsAdded shows if bookmark is being added or removed
// (true  => added)
// (false => removed)
type BookmarksEventDescription struct {
	EventTime string `json:"event_time"`
	Username  string `json:"username"`
	ID        string `json:"id"`
	EventType string `json:"event_type"`
	IsAdded   bool   `json:"is_added"`
}
