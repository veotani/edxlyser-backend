package models

// SequentialLog is a definition of a log object with event type seq_goto, seq_next, seq_prev or edx.ui.lms.link_clicked
type SequentialLog struct {
	Username          string             `json:"username"`
	EventType         string             `json:"event_type"`
	Time              string             `json:"time"`
	Event             SequentialEventLog `json:"event"`
	SequentialContext LogContext         `json:"context"`
}

// SequentialEventLog is a definition of an event object within SequentialLog
type SequentialEventLog struct {
	Old int `json:"old"`
	New int `json:"new"`
}
