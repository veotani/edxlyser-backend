package models

// Curve describes mathematical curve to be displayed in the frontend
// Points of this curve are int
type Curve struct {
	X []int `json:"x"`
	Y []int `json:"y"`
}

// CurveFloatToInt describes mathematical curve to be displayed in the frontend
// Points of this curve are ints in Y and floating in X
type CurveFloatToInt struct {
	X []float64 `json:"x"`
	Y []int     `json:"y"`
}
