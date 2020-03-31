package models

// LinkEventDescription has all the data about moving within sequential object
type LinkEventDescription struct {
	EventTime  string `json:"event_time"`
	Username   string `json:"username"`
	EventType  string `json:"event_type"`
	CurrentURL string `json:"current_url"`
	TargetURL  string `json:"target_url"`
}
