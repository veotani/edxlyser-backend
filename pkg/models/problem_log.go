package models

// ProblemLog is a definition of a log object with event type "edx.grades.problem.submitted", "problem_show" or "showanswer"
type ProblemLog struct {
	Username  string          `json:"username"`
	EventType string          `json:"event_type"`
	Time      string          `json:"time"`
	Event     ProblemEventLog `json:"event"`
}

// ProblemEventLog is a definition of an event object within ProblemLog
type ProblemEventLog struct {
	ProblemID        string  `json:"problem_id"`
	WeightedEarned   float64 `json:"weighted_earned"`
	WeightedPossible float64 `json:"weighted_possible"`
	Problem          string  `json:"problem"`
}
