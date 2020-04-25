package analysers

import (
	"fmt"
	"kafka-log-processor/pkg/models"
)

// GetAnalyseUserVideoWatchings represents intervals and values of how much people
// watched that fragment (if one person watches this fragment for two times, then
// it counts as two)
func (a *Analyser) GetAnalyseUserVideoWatchings(videoID string) (*models.CurveFloatToInt, error) {
	videoEvents, err := a.elasticService.GetSortedVideoEventsForVideo(videoID)
	fmt.Println(videoEvents)
	if err != nil {
		return nil, err
	}

	X := make([]float64, 0)
	Y := make([]int, 0)

	var previousX float64 // initialized with float64 zero value which is zero
	currentWatchers := 0
	for _, videoEvent := range videoEvents {
		if videoEvent.EventType == models.PLAY {
			currentWatchers++
		} else {
			currentWatchers--
		}

		if previousX != videoEvent.VideoTime {
			X = append(X, previousX)
			Y = append(Y, currentWatchers)
		}

		previousX = videoEvent.VideoTime
	}

	if videoEvents[len(videoEvents)-1].EventType == models.PLAY {
		currentWatchers++
	} else {
		currentWatchers--
	}

	X = append(X, previousX)
	Y = append(Y, currentWatchers)

	return &models.CurveFloatToInt{X: X, Y: Y}, nil
}
