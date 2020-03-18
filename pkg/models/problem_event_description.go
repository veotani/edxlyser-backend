package models

// ProblemEventDescription has all the data about video events for analysis
// JSON names are also mentioned for umarshaling and sending that json to elastic
type ProblemEventDescription struct {
	EventTime        string  `json:"event_time"`
	Username         string  `json:"username"`
	ProblemID        string  `json:"problem_id"`
	EventType        string  `json:"event_type"`
	WeightedEarned   float64 `json:"weighted_earned"`
	WeightedPossible float64 `json:"weighted_possible"`
}
