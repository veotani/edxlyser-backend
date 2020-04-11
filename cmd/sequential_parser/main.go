package main

import (
	"kafka-log-processor/configs"
	"kafka-log-processor/pkg/database"
	"kafka-log-processor/pkg/kafka"
	"kafka-log-processor/pkg/parsers"
	"log"
)

func main() {
	config, err := configs.GetParserConfig("./configs/parser_config.yml")
	if err != nil {
		log.Fatalln(err)
	}

	kafkaService := kafka.NewSequentialTopicKafka(config.Kafka.Host, config.Kafka.Port)
	es := database.ElasticService{}
	if err = es.Connect(config.Elastic.Host, config.Elastic.Port); err != nil {
		log.Fatal(err)
	}

	for {
		eventLog, err := kafkaService.NextMessage()
		if err != nil {
			log.Println("Cannot read next message from Kafka")
			log.Println(err)
			continue
		}

		sequentialEvent, err := parsers.ParseSequentialEvent(eventLog)
		if err != nil {
			log.Println("Cannot parse message from Kafka")
			log.Println(err)
			continue
		}

		if err = es.AddSequentialMoveEventDescription(sequentialEvent); err != nil {
			log.Println("cannot save event to elasticsearch")
			log.Println(err)
			continue
		}
	}
}
