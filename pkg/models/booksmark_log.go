package models

// BookmarksLog is a definition of a log object with event type edx.bookmark.removed, edx.bookmark.added:
type BookmarksLog struct {
	Username         string            `json:"username"`
	EventType        string            `json:"event_type"`
	Time             string            `json:"time"`
	Event            BookmarksEventLog `json:"event"`
	BookmarksContext LogContext        `json:"context"`
}

// BookmarksEventLog is a definition of an event object within BookmarksLog
type BookmarksEventLog struct {
	ComponentUsageID string `json:"component_usage_id"`
}
