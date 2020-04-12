package models

// LinkLog is a definition of a log object with event type "edx.ui.lms.link_clicked"
type LinkLog struct {
	Username    string       `json:"username"`
	EventType   string       `json:"event_type"`
	Time        string       `json:"time"`
	Event       LinkEventLog `json:"event"`
	LinkContext LogContext   `json:"context"`
}

// LinkEventLog is a definition of an event object within ProblemLog
type LinkEventLog struct {
	TargetURL  string `json:"target_url"`
	CurrentURL string `json:"current_url"`
}
