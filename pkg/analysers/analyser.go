package analysers

import (
	"kafka-log-processor/configs"
	"kafka-log-processor/pkg/database"
)

// Analyser contains analysis methods on logs
type Analyser struct {
	elasticService database.ElasticService
}

// New constructs analyser connected to ES
func New(config configs.ParserConfig) (*Analyser, error) {
	analyser := Analyser{}
	analyser.elasticService = database.ElasticService{}
	err := analyser.elasticService.Connect(config.Elastic.Host, config.Elastic.Port)
	if err != nil {
		return nil, err
	}
	return &analyser, nil
}
